"""
Scheduled cleanup management module for managing automatic cleanup tasks
"""
import logging
from typing import List, Dict, Any
from aiogram import Router, F
from aiogram.types import CallbackQuery, Message
from aiogram.filters import StateFilter
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from app.keys import BotKeys, SelectCB, Pages, Actions, SelectAll
from app.db import crud
from app.settings.language import MessageTexts
from app.settings.track import tracker
from app.models.server import ServerTypes
from app.api import ClinetManager
from app.scheduler.cleanup_scheduler import cleanup_scheduler
from app.routers.actions.items.bulk_cleanup import get_status_options

router = Router(name="scheduled_cleanup")
logger = logging.getLogger(__name__)


class ScheduledCleanupForm(StatesGroup):
    """States for scheduled cleanup workflow"""
    SELECT_ADMINS = State()
    SELECT_STATUS = State()
    SET_INTERVAL = State()
    CONFIRM = State()
    MANAGE_TASKS = State()


@router.callback_query(
    SelectCB.filter(
        (F.types == Pages.ACTIONS)
        & (F.action == Actions.INFO)
        & (F.select == "Scheduled Cleanup")
    )
)
async def show_scheduled_cleanup_menu(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Show scheduled cleanup management menu"""
    server = await crud.get_server(callback_data.panel)
    if not server:
        track = await callback.message.edit_text(
            text=MessageTexts.NOT_FOUND,
            reply_markup=BotKeys.cancel()
        )
        return await tracker.add(track)
    
    # Get existing tasks for this server
    all_tasks = await cleanup_scheduler.get_tasks()
    server_tasks = {k: v for k, v in all_tasks.items() if v.server_id == server.id}
    
    menu_options = [
        "➕ Create New Task",
        "📋 Manage Existing Tasks"
    ]
    
    tasks_info = ""
    if server_tasks:
        tasks_info = f"\n\n📊 Current Tasks: {len(server_tasks)}"
        active_tasks = sum(1 for task in server_tasks.values() if task.enabled)
        tasks_info += f"\n✅ Active: {active_tasks}"
        tasks_info += f"\n⏸️ Inactive: {len(server_tasks) - active_tasks}"
    
    return await callback.message.edit_text(
        text=f"⏰ Scheduled Cleanup Management\n\nServer: {server.data.get('host', 'Unknown')}{tasks_info}\n\nChoose an option:",
        reply_markup=BotKeys.selector(
            data=menu_options,
            types=Pages.BULK_CONFIG,
            action=Actions.SELECT_ADMIN,  # Reuse existing action
            panel=server.id,
            server_back=server.id
        )
    )


@router.callback_query(
    SelectCB.filter(
        (F.types == Pages.BULK_CONFIG)
        & (F.action == Actions.SELECT_ADMIN)
        & (F.select == "➕ Create New Task")
    )
)
async def start_create_scheduled_task(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Start creating a new scheduled cleanup task"""
    server = await crud.get_server(callback_data.panel)
    if not server:
        track = await callback.message.edit_text(
            text=MessageTexts.NOT_FOUND,
            reply_markup=BotKeys.cancel()
        )
        return await tracker.add(track)
    
    await state.set_state(ScheduledCleanupForm.SELECT_ADMINS)
    await state.update_data(selected_admins=[], server_type=server.types, server_id=server.id)
    
    # Get admins for selection
    admins = await ClinetManager.get_admins(server=server)
    if not admins:
        track = await callback.message.edit_text(
            text="❌ No admins found",
            reply_markup=BotKeys.cancel(server_back=server.id)
        )
        return await tracker.add(track)
    
    # Show admin selection with checkboxes
    admin_list = [admin.username for admin in admins]
    
    return await callback.message.edit_text(
        text="⏰ Create Scheduled Cleanup Task\n\n📋 Select Admins\n\nChoose admins whose users will be automatically cleaned up:",
        reply_markup=BotKeys.selector(
            data=admin_list,
            types=Pages.BULK_CONFIG,
            action=Actions.SELECT_ADMIN,
            panel=server.id,
            selects=[],
            all_selects=True,
            server_back=server.id
        )
    )


@router.callback_query(
    SelectCB.filter(
        (F.types == Pages.BULK_CONFIG)
        & (F.action == Actions.SELECT_ADMIN)
        & (F.select == "📋 Manage Existing Tasks")
    )
)
async def show_existing_tasks(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Show existing scheduled tasks"""
    server = await crud.get_server(callback_data.panel)
    if not server:
        track = await callback.message.edit_text(
            text=MessageTexts.NOT_FOUND,
            reply_markup=BotKeys.cancel()
        )
        return await tracker.add(track)
    
    # Get existing tasks for this server
    all_tasks = await cleanup_scheduler.get_tasks()
    server_tasks = {k: v for k, v in all_tasks.items() if v.server_id == server.id}
    
    if not server_tasks:
        return await callback.message.edit_text(
            text="📋 No scheduled tasks found for this server.\n\nCreate a new task to get started!",
            reply_markup=BotKeys.cancel(server_back=server.id)
        )
    
    # Create task list for selection
    task_options = []
    for task_id, task in server_tasks.items():
        status_emoji = "✅" if task.enabled else "⏸️"
        admins_text = ", ".join(task.admin_usernames[:2])
        if len(task.admin_usernames) > 2:
            admins_text += f" +{len(task.admin_usernames) - 2}"
        
        task_options.append((
            f"{status_emoji} {task_id} | {admins_text} | {task.interval_hours}h",
            task_id
        ))
    
    await state.set_state(ScheduledCleanupForm.MANAGE_TASKS)
    await state.update_data(server_id=server.id)
    
    return await callback.message.edit_text(
        text="📋 Existing Scheduled Tasks\n\nSelect a task to manage:",
        reply_markup=BotKeys.selector(
            data=task_options,
            types=Pages.BULK_CONFIG,
            action=Actions.SELECT_SERVICE,
            panel=server.id,
            server_back=server.id
        )
    )


@router.callback_query(
    StateFilter(ScheduledCleanupForm.MANAGE_TASKS),
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.SELECT_SERVICE))
    )
)
async def manage_specific_task(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Manage a specific scheduled task"""
    task_id = callback_data.select
    task = await cleanup_scheduler.get_task(task_id)
    
    if not task:
        await callback.answer("❌ Task not found", show_alert=True)
        return
    
    # Create management options
    options = []
    if task.enabled:
        options.append("⏸️ Disable Task")
    else:
        options.append("▶️ Enable Task")
    
    options.extend([
        "🗑️ Delete Task",
        "📊 View Details"
    ])
    
    status_text = "✅ Active" if task.enabled else "⏸️ Inactive"
    admins_text = ", ".join(task.admin_usernames)
    next_run_text = task.next_run.strftime("%Y-%m-%d %H:%M") if task.next_run else "Not scheduled"
    
    return await callback.message.edit_text(
        text=(
            f"⚙️ Manage Task: {task_id}\n\n"
            f"Status: {status_text}\n"
            f"Admins: {admins_text}\n"
            f"Interval: {task.interval_hours} hours\n"
            f"Next Run: {next_run_text}\n\n"
            f"Choose an action:"
        ),
        reply_markup=BotKeys.selector(
            data=options,
            types=Pages.BULK_CONFIG,
            action=Actions.CONFIRM,
            panel=f"{callback_data.panel}:{task_id}",  # Encode task_id in panel
            server_back=callback_data.panel
        )
    )


@router.callback_query(
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.CONFIRM))
        & (F.select.in_(["⏸️ Disable Task", "▶️ Enable Task", "🗑️ Delete Task", "📊 View Details"]))
    )
)
async def execute_task_action(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Execute action on scheduled task"""
    # Extract server_id and task_id from panel
    panel_parts = callback_data.panel.split(":")
    server_id = int(panel_parts[0])
    task_id = panel_parts[1] if len(panel_parts) > 1 else None
    
    if not task_id:
        await callback.answer("❌ Invalid task", show_alert=True)
        return
    
    action = callback_data.select
    success = False
    message = ""
    
    if action == "⏸️ Disable Task":
        success = await cleanup_scheduler.disable_task(task_id)
        message = "✅ Task disabled successfully" if success else "❌ Failed to disable task"
    
    elif action == "▶️ Enable Task":
        success = await cleanup_scheduler.enable_task(task_id)
        message = "✅ Task enabled successfully" if success else "❌ Failed to enable task"
    
    elif action == "🗑️ Delete Task":
        success = await cleanup_scheduler.remove_task(task_id)
        message = "✅ Task deleted successfully" if success else "❌ Failed to delete task"
    
    elif action == "📊 View Details":
        task = await cleanup_scheduler.get_task(task_id)
        if task:
            last_run_text = task.last_run.strftime("%Y-%m-%d %H:%M") if task.last_run else "Never"
            next_run_text = task.next_run.strftime("%Y-%m-%d %H:%M") if task.next_run else "Not scheduled"
            created_text = task.created_at.strftime("%Y-%m-%d %H:%M") if task.created_at else "Unknown"
            
            details_text = (
                f"📊 Task Details: {task_id}\n\n"
                f"Status: {'✅ Active' if task.enabled else '⏸️ Inactive'}\n"
                f"Server ID: {task.server_id}\n"
                f"Admins: {', '.join(task.admin_usernames)}\n"
                f"Status Filters: {', '.join(task.status_filters)}\n"
                f"Interval: {task.interval_hours} hours\n"
                f"Created: {created_text}\n"
                f"Last Run: {last_run_text}\n"
                f"Next Run: {next_run_text}"
            )
            
            return await callback.message.edit_text(
                text=details_text,
                reply_markup=BotKeys.cancel(server_back=server_id)
            )
        else:
            message = "❌ Task not found"
    
    await callback.answer(message, show_alert=True)
    
    # Return to task list if action was successful
    if success and action != "📊 View Details":
        # Redirect back to existing tasks view
        return await show_existing_tasks(callback, SelectCB(
            types=Pages.BULK_CONFIG,
            action=Actions.SELECT_ADMIN,
            select="📋 Manage Existing Tasks",
            panel=server_id
        ), state)


# Continue with the admin selection handlers for creating new tasks
@router.callback_query(
    StateFilter(ScheduledCleanupForm.SELECT_ADMINS),
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.SELECT_ADMIN))
        & (F.done.is_not(True))
    )
)
async def toggle_admin_selection_scheduled(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Handle admin checkbox toggle for scheduled cleanup"""
    data = await state.get_data()
    selected_admins = data.get("selected_admins", [])
    server = await crud.get_server(callback_data.panel)
    
    if not server:
        track = await callback.message.edit_text(
            text=MessageTexts.NOT_FOUND,
            reply_markup=BotKeys.cancel()
        )
        return await tracker.add(track)
    
    # Get all admins for the list
    admins = await ClinetManager.get_admins(server=server)
    admin_list = [admin.username for admin in admins]
    
    # Handle select all/deselect all
    if callback_data.select == SelectAll.SELECT:
        selected_admins = admin_list.copy()
    elif callback_data.select == SelectAll.DESELECT:
        selected_admins = []
    else:
        # Toggle individual selection
        if callback_data.select in selected_admins:
            selected_admins.remove(callback_data.select)
        else:
            selected_admins.append(callback_data.select)
    
    await state.update_data(selected_admins=selected_admins)
    
    # Update the keyboard with new selection
    return await callback.message.edit_text(
        text=f"⏰ Create Scheduled Cleanup Task\n\n📋 Select Admins\n\nSelected: {len(selected_admins)}/{len(admin_list)}\n\nChoose admins whose users will be automatically cleaned up:",
        reply_markup=BotKeys.selector(
            data=admin_list,
            types=Pages.BULK_CONFIG,
            action=Actions.SELECT_ADMIN,
            panel=server.id,
            selects=selected_admins,
            all_selects=True,
            server_back=server.id
        )
    )


@router.callback_query(
    StateFilter(ScheduledCleanupForm.SELECT_ADMINS),
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.SELECT_ADMIN))
        & (F.done.is_(True))
    )
)
async def admins_selected_scheduled(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Proceed to status selection after admins are selected"""
    data = await state.get_data()
    selected_admins = data.get("selected_admins", [])
    server_type = data.get("server_type")
    
    if not selected_admins:
        await callback.answer("⚠️ Please select at least one admin", show_alert=True)
        return
    
    server = await crud.get_server(callback_data.panel)
    if not server:
        track = await callback.message.edit_text(
            text=MessageTexts.NOT_FOUND,
            reply_markup=BotKeys.cancel()
        )
        return await tracker.add(track)
    
    await state.set_state(ScheduledCleanupForm.SELECT_STATUS)
    await state.update_data(selected_statuses=[])
    
    # Get status options based on server type
    status_options = get_status_options(server_type)
    
    admins_text = ", ".join(selected_admins[:5])
    if len(selected_admins) > 5:
        admins_text += f" and {len(selected_admins) - 5} more"
    
    return await callback.message.edit_text(
        text=f"⏰ Create Scheduled Cleanup Task\n\n🏷️ Select Status Filters\n\nAdmins: {admins_text}\n\nChoose user statuses to automatically delete:",
        reply_markup=BotKeys.selector(
            data=status_options,
            types=Pages.BULK_CONFIG,
            action=Actions.SELECT_SERVICE,
            panel=server.id,
            selects=[],
            all_selects=True,
            server_back=server.id
        )
    )


@router.callback_query(
    StateFilter(ScheduledCleanupForm.SELECT_STATUS),
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.SELECT_SERVICE))
        & (F.done.is_not(True))
    )
)
async def toggle_status_selection_scheduled(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Handle status checkbox toggle for scheduled cleanup"""
    data = await state.get_data()
    selected_statuses = data.get("selected_statuses", [])
    selected_admins = data.get("selected_admins", [])
    server_type = data.get("server_type")
    server = await crud.get_server(callback_data.panel)
    
    if not server:
        track = await callback.message.edit_text(
            text=MessageTexts.NOT_FOUND,
            reply_markup=BotKeys.cancel()
        )
        return await tracker.add(track)
    
    # Get status options
    status_options = get_status_options(server_type)
    status_values = [option[1] for option in status_options]
    
    # Handle select all/deselect all
    if callback_data.select == SelectAll.SELECT:
        selected_statuses = status_values.copy()
    elif callback_data.select == SelectAll.DESELECT:
        selected_statuses = []
    else:
        # Toggle individual selection
        if callback_data.select in selected_statuses:
            selected_statuses.remove(callback_data.select)
        else:
            selected_statuses.append(callback_data.select)
    
    await state.update_data(selected_statuses=selected_statuses)
    
    admins_text = ", ".join(selected_admins[:5])
    if len(selected_admins) > 5:
        admins_text += f" and {len(selected_admins) - 5} more"
    
    # Update the keyboard with new selection
    return await callback.message.edit_text(
        text=f"⏰ Create Scheduled Cleanup Task\n\n🏷️ Select Status Filters\n\nAdmins: {admins_text}\nSelected: {len(selected_statuses)}/{len(status_options)}\n\nChoose user statuses to automatically delete:",
        reply_markup=BotKeys.selector(
            data=status_options,
            types=Pages.BULK_CONFIG,
            action=Actions.SELECT_SERVICE,
            panel=server.id,
            selects=selected_statuses,
            all_selects=True,
            server_back=server.id
        )
    )


@router.callback_query(
    StateFilter(ScheduledCleanupForm.SELECT_STATUS),
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.SELECT_SERVICE))
        & (F.done.is_(True))
    )
)
async def statuses_selected_scheduled(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Proceed to interval setting after statuses are selected"""
    data = await state.get_data()
    selected_statuses = data.get("selected_statuses", [])
    
    if not selected_statuses:
        await callback.answer("⚠️ Please select at least one status", show_alert=True)
        return
    
    await state.set_state(ScheduledCleanupForm.SET_INTERVAL)
    
    # Show interval options
    interval_options = [
        ("⏰ Every 1 Hour", "1"),
        ("🕐 Every 2 Hours", "2"),
        ("🕕 Every 6 Hours", "6"),
        ("🕛 Every 12 Hours", "12"),
        ("📅 Every 24 Hours (Daily)", "24"),
        ("📅 Every 48 Hours (2 Days)", "48"),
        ("📅 Every 72 Hours (3 Days)", "72"),
        ("📅 Every 168 Hours (Weekly)", "168")
    ]
    
    selected_admins = data.get("selected_admins", [])
    admins_text = ", ".join(selected_admins[:3])
    if len(selected_admins) > 3:
        admins_text += f" and {len(selected_admins) - 3} more"
    
    return await callback.message.edit_text(
        text=f"⏰ Create Scheduled Cleanup Task\n\n⏱️ Set Cleanup Interval\n\nAdmins: {admins_text}\nStatus Filters: {len(selected_statuses)} selected\n\nHow often should the cleanup run?",
        reply_markup=BotKeys.selector(
            data=interval_options,
            types=Pages.BULK_CONFIG,
            action=Actions.CONFIRM,
            panel=callback_data.panel,
            server_back=callback_data.panel
        )
    )


@router.callback_query(
    StateFilter(ScheduledCleanupForm.SET_INTERVAL),
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.CONFIRM))
    )
)
async def interval_selected(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Show confirmation after interval is selected"""
    interval_hours = int(callback_data.select)
    await state.update_data(interval_hours=interval_hours)
    
    data = await state.get_data()
    selected_admins = data.get("selected_admins", [])
    selected_statuses = data.get("selected_statuses", [])
    server_type = data.get("server_type")
    
    # Get status names for display
    status_options = get_status_options(server_type)
    status_names = []
    for status_value in selected_statuses:
        status_option = next((opt for opt in status_options if opt[1] == status_value), None)
        if status_option:
            status_names.append(status_option[0])
    
    # Prepare confirmation message
    admins_text = ", ".join(selected_admins[:3])
    if len(selected_admins) > 3:
        admins_text += f" and {len(selected_admins) - 3} more"
    
    statuses_text = ", ".join(status_names[:3])
    if len(status_names) > 3:
        statuses_text += f" and {len(status_names) - 3} more"
    
    interval_text = f"{interval_hours} hour{'s' if interval_hours > 1 else ''}"
    if interval_hours == 24:
        interval_text = "Daily"
    elif interval_hours == 168:
        interval_text = "Weekly"
    
    confirmation_text = (
        f"⚠️ Confirm Scheduled Cleanup Task\n\n"
        f"👥 Admins: {admins_text}\n"
        f"🏷️ Status Filters: {statuses_text}\n"
        f"⏱️ Interval: {interval_text}\n\n"
        f"⚠️ This will automatically delete users matching the criteria every {interval_text.lower()}!\n\n"
        f"Do you want to create this scheduled task?"
    )
    
    await state.set_state(ScheduledCleanupForm.CONFIRM)
    
    return await callback.message.edit_text(
        text=confirmation_text,
        reply_markup=BotKeys.selector(
            data=["✅ Create Task", "❌ Cancel"],
            types=Pages.BULK_CONFIG,
            action=Actions.CONFIRM,
            panel=callback_data.panel,
            server_back=callback_data.panel
        )
    )


@router.callback_query(
    StateFilter(ScheduledCleanupForm.CONFIRM),
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.CONFIRM))
        & (F.select.in_(["✅ Create Task", "❌ Cancel"]))
    )
)
async def create_scheduled_task(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Create the scheduled cleanup task"""
    if callback_data.select == "❌ Cancel":
        track = await callback.message.edit_text(
            text="❌ Scheduled task creation cancelled",
            reply_markup=BotKeys.cancel(server_back=callback_data.panel)
        )
        await state.clear()
        return await tracker.add(track)
    
    server = await crud.get_server(callback_data.panel)
    if not server:
        track = await callback.message.edit_text(
            text=MessageTexts.NOT_FOUND,
            reply_markup=BotKeys.cancel()
        )
        await state.clear()
        return await tracker.add(track)
    
    data = await state.get_data()
    selected_admins = data.get("selected_admins", [])
    selected_statuses = data.get("selected_statuses", [])
    interval_hours = data.get("interval_hours", 24)
    
    # Generate task ID
    import time
    task_id = f"cleanup_{server.id}_{int(time.time())}"
    
    # Create the scheduled task
    success = await cleanup_scheduler.add_task(
        task_id=task_id,
        server_id=server.id,
        admin_usernames=selected_admins,
        status_filters=selected_statuses,
        interval_hours=interval_hours
    )
    
    if success:
        interval_text = f"{interval_hours} hour{'s' if interval_hours > 1 else ''}"
        if interval_hours == 24:
            interval_text = "daily"
        elif interval_hours == 168:
            interval_text = "weekly"
        
        result_text = (
            f"✅ Scheduled Cleanup Task Created!\n\n"
            f"Task ID: {task_id}\n"
            f"👥 Admins: {len(selected_admins)} selected\n"
            f"🏷️ Status Filters: {len(selected_statuses)} selected\n"
            f"⏱️ Runs: Every {interval_text}\n\n"
            f"The task is now active and will run automatically."
        )
    else:
        result_text = "❌ Failed to create scheduled task. Please try again."
    
    track = await callback.message.edit_text(
        text=result_text,
        reply_markup=BotKeys.cancel(server_back=server.id)
    )
    
    await state.clear()
    return await tracker.add(track)
