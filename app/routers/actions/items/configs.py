import asyncio
import logging
from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.filters import StateFilter
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from app.keys import BotKeys, SelectCB, Pages, Actions
from app.db import crud
from app.settings.language import MessageTexts
from app.settings.track import tracker
from app.models.action import ActionTypes
from app.api import ClinetManager
from app.api.types.marzneshin import MarzneshinUserResponse, UserExpireStrategy
from .config_helper import prepare_user_modify_data, validate_user_data, log_user_modification

logger = logging.getLogger(__name__)

router = Router(name="actions_add_config")


class ConfigsActionsForm(StatesGroup):
    ADMINS = State()
    CONFIGS = State()


@router.callback_query(
    SelectCB.filter(
        (F.types.is_(Pages.ACTIONS))
        & (F.action.is_(Actions.INFO))
        & (
            F.select.in_(
                [ActionTypes.ADD_CONFIG.value, ActionTypes.DELETE_CONFIG.value]
            )
        )
    )
)
async def select(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    server = await crud.get_server(callback_data.panel)
    if not server:
        track = await callback.message.edit_text(
            text=MessageTexts.NOT_FOUND, reply_markup=BotKeys.cancel()
        )
        return await tracker.add(track)

    await state.set_state(ConfigsActionsForm.ADMINS)
    admins = await ClinetManager.get_admins(server=server)
    if not admins:
        track = await callback.message.edit_text(
            text=MessageTexts.NOT_FOUND,
            reply_markup=BotKeys.cancel(server_back=server.id),
        )
        return await tracker.add(track)

    await state.update_data(action=callback_data.select)
    return await callback.message.edit_text(
        text=MessageTexts.ASK_ADMIN,
        reply_markup=BotKeys.selector(
            data=[admin.username for admin in admins] + ["ALL"],
            types=Pages.ACTIONS,
            action=Actions.INFO,
            panel=server.id,
            server_back=server.id,
        ),
    )


@router.callback_query(
    StateFilter(ConfigsActionsForm.ADMINS),
    SelectCB.filter((F.types.is_(Pages.ACTIONS)) & (F.action.is_(Actions.INFO))),
)
async def admin(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    await state.update_data(admin=callback_data.select)

    server = await crud.get_server(callback_data.panel)
    if not server:
        track = await callback.message.edit_text(
            text=MessageTexts.NOT_FOUND, reply_markup=BotKeys.cancel()
        )
        return await tracker.add(track)

    await state.set_state(ConfigsActionsForm.CONFIGS)
    configs = await ClinetManager.get_configs(server)
    if not configs:
        track = await callback.message.edit_text(
            text=MessageTexts.NOT_FOUND,
            reply_markup=BotKeys.cancel(server_back=server.id),
        )
        return await tracker.add(track)

    await state.update_data(configs=[config.dict() for config in configs])
    return await callback.message.edit_text(
        text=MessageTexts.ITEMS,
        reply_markup=BotKeys.selector(
            data=[config.remark for config in configs],
            types=Pages.ACTIONS,
            action=Actions.INFO,
            panel=server.id,
            width=1,
            server_back=server.id,
        ),
    )


@router.callback_query(
    StateFilter(ConfigsActionsForm.CONFIGS),
    SelectCB.filter((F.types.is_(Pages.ACTIONS)) & (F.action.is_(Actions.INFO))),
)
async def action(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    server = await crud.get_server(callback_data.panel)
    if not server:
        track = await callback.message.edit_text(
            text=MessageTexts.NOT_FOUND, reply_markup=BotKeys.cancel()
        )
        return await tracker.add(track)

    data = await state.get_data()
    configs = data["configs"]
    selected = callback_data.select
    target_config = next(
        (config for config in configs if config["remark"] == selected), None
    )
    if not target_config:
        track = await callback.message.edit_text(
            text=MessageTexts.NOT_FOUND,
            reply_markup=BotKeys.cancel(server_back=server.id),
        )
        return await tracker.add(track)

    # Send progress message
    progress_msg = await callback.message.edit_text(text="⏳ Processing users...")

    target = int(target_config["id"])
    action_type = data["action"]
    adminselect = data["admin"]

    async def process_user(user: MarzneshinUserResponse):
        """Process a single user - add or remove service"""
        try:
            # Validate user data first
            validation_error = validate_user_data(user)
            if validation_error:
                logger.warning(validation_error)
            
            # Check if action is needed
            if action_type == ActionTypes.ADD_CONFIG.value:
                if target in user.service_ids:
                    return None  # Already has the service
                user.service_ids.append(target)
                action_name = "add"
            elif action_type == ActionTypes.DELETE_CONFIG.value:
                if target not in user.service_ids:
                    return None  # Doesn't have the service
                user.service_ids.remove(target)
                action_name = "remove"
            else:
                return None

            # Use helper to prepare data with all fields preserved
            modify_data = prepare_user_modify_data(user, preserve_all=True)
            
            # Call API to modify user
            result = await ClinetManager.modify_user(
                server=server,
                username=user.username,
                data=modify_data,
            )
            
            # Log the result
            log_user_modification(
                username=user.username,
                action=action_name,
                service_id=target,
                success=bool(result),
                error=None if result else "API call failed"
            )
            
            return result
        except Exception as e:
            logger.error(f"Error processing user {user.username}: {e}")
            log_user_modification(
                username=user.username,
                action=action_type,
                service_id=target,
                success=False,
                error=str(e)
            )
            return None

    # Process users in batches
    page = 1
    total_processed = 0
    success_count = 0
    failed_count = 0
    batch_size = 10  # Process 10 users concurrently

    while True:
        # Get users page
        users = await ClinetManager.get_users(
            server,
            page,
            size=server.size_value,
            owner_username=None if adminselect == "ALL" else adminselect,
        )
        if not users:
            break

        # Process users in smaller batches for better performance
        for i in range(0, len(users), batch_size):
            batch = users[i:i+batch_size]
            results = await asyncio.gather(
                *(process_user(user) for user in batch),
                return_exceptions=True
            )
            
            for result in results:
                if result is not None:
                    if isinstance(result, Exception):
                        failed_count += 1
                    elif result:
                        success_count += 1
                    else:
                        failed_count += 1
                total_processed += 1
            
            # Update progress every batch
            if total_processed % 50 == 0:
                await progress_msg.edit_text(
                    text=f"⏳ Processing... {total_processed} users processed"
                )

        page += 1

    # Send final result
    action_text = "Added" if action_type == ActionTypes.ADD_CONFIG.value else "Removed"
    result_text = (
        f"✅ Action Completed!\n\n"
        f"Service: {target_config['remark']}\n"
        f"Action: {action_text}\n"
        f"Admin: {adminselect}\n"
        f"Success: {success_count}\n"
        f"Failed: {failed_count}\n"
        f"Total Processed: {total_processed}"
    )
    
    track = await callback.message.answer(
        text=result_text,
        reply_markup=BotKeys.cancel(server_back=server.id),
    )
    return await tracker.cleardelete(callback, track)
