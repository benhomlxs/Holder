"""
Bulk service assignment module for managing multiple services and admins
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.filters import StateFilter
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from app.keys import BotKeys, SelectCB, Pages, Actions, SelectAll
from app.db import crud
from app.settings.language import MessageTexts
from app.settings.track import tracker
from app.models.action import ActionTypes
from app.api import ClinetManager
from app.api.types.marzneshin import MarzneshinUserResponse
from .config_helper import prepare_user_modify_data, validate_user_data, log_user_modification

router = Router(name="bulk_configs")
logger = logging.getLogger(__name__)

# Configure logging to see debug messages
logging.basicConfig(level=logging.INFO)


class BulkConfigForm(StatesGroup):
    """States for bulk configuration workflow"""
    ACTION_TYPE = State()
    SELECT_ADMINS = State()
    SELECT_SERVICES = State()
    CONFIRM = State()
    PROCESSING = State()


class CircuitBreaker:
    """Simple circuit breaker to prevent server overload"""
    
    def __init__(self, failure_threshold: int = 8, recovery_timeout: int = 20):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def can_execute(self) -> bool:
        """Check if request can be executed"""
        if self.state == "CLOSED":
            return True
        elif self.state == "OPEN":
            if self.last_failure_time and \
               datetime.now() - self.last_failure_time > timedelta(seconds=self.recovery_timeout):
                self.state = "HALF_OPEN"
                return True
            return False
        else:  # HALF_OPEN
            return True
    
    def record_success(self):
        """Record successful request"""
        self.failure_count = 0
        self.state = "CLOSED"
    
    def record_failure(self):
        """Record failed request"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            logger.warning(f"Circuit breaker opened after {self.failure_count} failures")


class BulkOperationManager:
    """Manager for bulk operations with optimized batch processing"""
    
    def __init__(self, batch_size: int = 12, concurrent_limit: int = 3, rate_limit_delay: float = 0.05):
        self.batch_size = batch_size
        self.concurrent_limit = concurrent_limit
        self.rate_limit_delay = rate_limit_delay  # Delay between requests
        self.progress_updates = {}
        self.circuit_breaker = CircuitBreaker(failure_threshold=6, recovery_timeout=15)
        
    async def process_bulk_assignment(
        self,
        server,
        admins: List[str],
        service_ids: List[int],
        action_type: str,
        progress_callback=None
    ) -> Dict[str, Any]:
        """
        Process bulk service assignment for multiple admins and services
        
        Args:
            server: Server object
            admins: List of admin usernames to process
            service_ids: List of service IDs to add/remove
            action_type: ADD_CONFIG or DELETE_CONFIG
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dictionary with operation results
        """
        results = {
            "total_users": 0,
            "total_operations": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "errors": [],
            "admin_results": {},
            "processed_users": set()  # Track unique users to avoid double counting
        }
        
        # Track processed user-service pairs to avoid double counting
        processed_operations = set()
        
        for admin in admins:
            admin_result = await self._process_admin_users(
                server, admin, service_ids, action_type, progress_callback, results["processed_users"]
            )
            results["admin_results"][admin] = admin_result
            results["total_users"] = len(results["processed_users"])  # Count unique users
            results["total_operations"] += admin_result["operations"]
            results["successful"] += admin_result["successful"]
            results["failed"] += admin_result["failed"]
            results["skipped"] += admin_result["skipped"]
            if admin_result["errors"]:
                results["errors"].extend(admin_result["errors"])
        
        # Validate final numbers for debugging
        total_expected = len(results["processed_users"]) * len(service_ids)
        total_actual = results["total_operations"] + results["skipped"]
        
        if total_expected != total_actual:
            logger.warning(f"Calculation mismatch: Expected {total_expected}, Got {total_actual}")
            logger.warning(f"Users: {len(results['processed_users'])}, Services: {len(service_ids)}")
            logger.warning(f"Operations: {results['total_operations']}, Skipped: {results['skipped']}")
        
        # Remove the set from final results (not needed in output)
        del results["processed_users"]
        return results
    
    async def _process_admin_users(
        self,
        server,
        admin: str,
        service_ids: List[int],
        action_type: str,
        progress_callback=None,
        processed_users_set=None
    ) -> Dict[str, Any]:
        """Process all users for a single admin"""
        result = {
            "total_users": 0,
            "operations": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "errors": []
        }
        
        if processed_users_set is None:
            processed_users_set = set()
        
        page = 1
        admin_users_count = 0
        
        while True:
            # Fetch users for this admin
            users = await ClinetManager.get_users(
                server,
                page,
                size=server.size_value,
                owner_username=admin
            )
            
            if not users:
                break
            
            # Filter users that haven't been processed yet (avoid duplicate processing)
            new_users = []
            for user in users:
                if user.username not in processed_users_set:
                    processed_users_set.add(user.username)
                    new_users.append(user)
                    admin_users_count += 1
                # If user already processed by another admin, skip processing
            
            # Process users in batches
            for i in range(0, len(new_users), self.batch_size):
                batch = new_users[i:i+self.batch_size]
                if not batch:  # Skip empty batches
                    continue
                
                batch_results = await self._process_user_batch(
                    server, batch, service_ids, action_type
                )
                
                result["operations"] += batch_results["operations"]
                result["successful"] += batch_results["successful"]
                result["failed"] += batch_results["failed"]
                result["skipped"] += batch_results["skipped"]
                if batch_results["errors"]:
                    result["errors"].extend(batch_results["errors"])
                
                # Send progress update if callback provided
                if progress_callback:
                    # Update with current unique user count
                    temp_result = result.copy()
                    temp_result["total_users"] = len(processed_users_set)
                    await progress_callback(admin, temp_result)
                    
            page += 1
            
        result["total_users"] = admin_users_count  # Users for this specific admin
        return result
    
    async def _process_user_batch(
        self,
        server,
        users: List[MarzneshinUserResponse],
        service_ids: List[int],
        action_type: str
    ) -> Dict[str, Any]:
        """Process a batch of users concurrently - optimized approach"""
        result = {
            "operations": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "errors": []
        }
        
        # Create tasks for users that need processing (pre-filter)
        tasks = []
        user_service_pairs = []
        
        for user in users:
            for service_id in service_ids:
                # Pre-check if action is needed to avoid unnecessary tasks
                needs_update = False
                if action_type == ActionTypes.ADD_CONFIG.value:
                    if service_id not in user.service_ids:
                        needs_update = True
                elif action_type == ActionTypes.DELETE_CONFIG.value:
                    if service_id in user.service_ids:
                        needs_update = True
                
                if needs_update:
                    task = self._process_single_user_service(
                        server, user, service_id, action_type
                    )
                    tasks.append(task)
                    user_service_pairs.append((user.username, service_id))
                else:
                    # Count skipped operations immediately
                    result["skipped"] += 1
        
        if not tasks:
            # All operations were skipped
            return result
        
        # Process with concurrency limit and better error handling
        semaphore = asyncio.Semaphore(self.concurrent_limit)
        
        async def limited_task(task):
            async with semaphore:
                try:
                    return await task
                except Exception as e:
                    logger.error(f"Task failed with exception: {e}")
                    return "failed"
        
        # Execute only necessary tasks
        results = await asyncio.gather(
            *(limited_task(task) for task in tasks),
            return_exceptions=False
        )
        
        # Count results - only actual API calls
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                result["operations"] += 1
                result["failed"] += 1
                result["errors"].append(str(res))
            elif res == "success":
                result["operations"] += 1
                result["successful"] += 1
            elif res == "failed":
                result["operations"] += 1
                result["failed"] += 1
            # Note: "skipped" results shouldn't happen here since we pre-filtered
                
        return result
    
    async def _process_single_user_service(
        self,
        server,
        user: MarzneshinUserResponse,
        service_id: int,
        action_type: str
    ) -> str:
        """Process a single service for a single user - only called when update is needed"""
        try:
            # Check circuit breaker
            if not self.circuit_breaker.can_execute():
                logger.warning(f"Circuit breaker is open, skipping {user.username}")
                return "failed"
            
            # Add minimal rate limiting delay only when needed
            if self.circuit_breaker.failure_count > 2:
                await asyncio.sleep(self.rate_limit_delay * 2)
            else:
                await asyncio.sleep(self.rate_limit_delay)
            
            # Validate user data
            validation_error = validate_user_data(user)
            if validation_error:
                logger.warning(f"Validation error for {user.username}: {validation_error}")
            
            # Apply the change (we know it's needed since we pre-filtered)
            original_service_ids = user.service_ids.copy()
            
            if action_type == ActionTypes.ADD_CONFIG.value:
                user.service_ids.append(service_id)
            elif action_type == ActionTypes.DELETE_CONFIG.value:
                user.service_ids.remove(service_id)
            
            # Prepare and send update with retry mechanism
            modify_data = prepare_user_modify_data(user, preserve_all=True)
            
            # Use the improved API client with retry
            result = await ClinetManager.modify_user(
                server=server,
                username=user.username,
                data=modify_data
            )
            
            # Update circuit breaker based on result
            if result:
                self.circuit_breaker.record_success()
                # Reduce delay on success for adaptive speed
                if self.rate_limit_delay > 0.02:
                    self.rate_limit_delay *= 0.95
            else:
                self.circuit_breaker.record_failure()
                # Increase delay on failure for stability
                if self.rate_limit_delay < 0.5:
                    self.rate_limit_delay *= 1.2
                # If API call failed, restore original service_ids
                user.service_ids = original_service_ids
            
            # Log the operation
            action_name = "add" if action_type == ActionTypes.ADD_CONFIG.value else "remove"
            log_user_modification(
                username=user.username,
                action=action_name,
                service_id=service_id,
                success=bool(result),
                error=None if result else "API call failed"
            )
            
            return "success" if result else "failed"
            
        except Exception as e:
            logger.error(f"Error processing {user.username} for service {service_id}: {e}")
            self.circuit_breaker.record_failure()
            return "failed"


# Initialize the bulk operation manager with optimized settings
bulk_manager = BulkOperationManager(
    batch_size=12,  # Balanced batch size for optimal throughput
    concurrent_limit=3,  # Moderate concurrency for speed without overload
    rate_limit_delay=0.05  # Minimal delay for maximum speed
)



@router.callback_query(
    SelectCB.filter(
        (F.types == Pages.ACTIONS)
        & (F.action == Actions.INFO)
        & (F.select == ActionTypes.ADD_CONFIG.value + " (Bulk)")
    )
)
async def start_bulk_add(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Start bulk add configuration workflow"""
    logger.info(f"Bulk add handler triggered - callback_data: {callback_data}")
    await _start_bulk_workflow(callback, callback_data, state, ActionTypes.ADD_CONFIG.value)


@router.callback_query(
    SelectCB.filter(
        (F.types == Pages.ACTIONS)
        & (F.action == Actions.INFO)
        & (F.select == ActionTypes.DELETE_CONFIG.value + " (Bulk)")
    )
)
async def start_bulk_delete(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Start bulk delete configuration workflow"""
    logger.info(f"Bulk delete handler triggered - callback_data: {callback_data}")
    await _start_bulk_workflow(callback, callback_data, state, ActionTypes.DELETE_CONFIG.value)


async def _start_bulk_workflow(
    callback: CallbackQuery,
    callback_data: SelectCB,
    state: FSMContext,
    action_type: str
):
    """Common workflow starter for bulk operations"""
    logger.info(f"Starting bulk workflow - action_type: {action_type}, panel: {callback_data.panel}")
    server = await crud.get_server(callback_data.panel)
    if not server:
        track = await callback.message.edit_text(
            text=MessageTexts.NOT_FOUND,
            reply_markup=BotKeys.cancel()
        )
        return await tracker.add(track)
    
    await state.set_state(BulkConfigForm.SELECT_ADMINS)
    await state.update_data(action=action_type, selected_admins=[])
    
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
        text="📋 Select Admins\n\nChoose one or more admins whose users will be affected:",
        reply_markup=BotKeys.selector(
            data=admin_list,
            types=Pages.BULK_CONFIG,
            action=Actions.SELECT_ADMIN,
            panel=server.id,
            selects=[],  # Start with empty selection
            all_selects=True,  # Enable select all/deselect all buttons
            server_back=server.id
        )
    )


@router.callback_query(
    StateFilter(BulkConfigForm.SELECT_ADMINS),
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.SELECT_ADMIN))
        & (F.done.is_not(True))
    )
)
async def toggle_admin_selection(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Handle admin checkbox toggle"""
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
        text=f"📋 Select Admins\n\nSelected: {len(selected_admins)}/{len(admin_list)}\n\nChoose admins whose users will be affected:",
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
    StateFilter(BulkConfigForm.SELECT_ADMINS),
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.SELECT_ADMIN))
        & (F.done.is_(True))
    )
)
async def admins_selected(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Proceed to service selection after admins are selected"""
    data = await state.get_data()
    selected_admins = data.get("selected_admins", [])
    
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
    
    await state.set_state(BulkConfigForm.SELECT_SERVICES)
    await state.update_data(selected_services=[])
    
    # Get services for selection
    configs = await ClinetManager.get_configs(server)
    if not configs:
        track = await callback.message.edit_text(
            text="❌ No services found",
            reply_markup=BotKeys.cancel(server_back=server.id)
        )
        return await tracker.add(track)
    
    # Store configs in state for later use
    await state.update_data(configs=[config.dict() for config in configs])
    
    # Show service selection with checkboxes
    service_list = [(config.remark, str(config.id)) for config in configs]
    
    admins_text = ", ".join(selected_admins[:5])
    if len(selected_admins) > 5:
        admins_text += f" and {len(selected_admins) - 5} more"
    
    return await callback.message.edit_text(
        text=f"🔧 Select Services\n\nAdmins: {admins_text}\n\nChoose services to assign/remove:",
        reply_markup=BotKeys.selector(
            data=service_list,
            types=Pages.BULK_CONFIG,
            action=Actions.SELECT_SERVICE,
            panel=server.id,
            selects=[],
            all_selects=True,
            server_back=server.id
        )
    )


@router.callback_query(
    StateFilter(BulkConfigForm.SELECT_SERVICES),
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.SELECT_SERVICE))
        & (F.done.is_not(True))
    )
)
async def toggle_service_selection(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Handle service checkbox toggle"""
    data = await state.get_data()
    selected_services = data.get("selected_services", [])
    configs = data.get("configs", [])
    server = await crud.get_server(callback_data.panel)
    
    if not server:
        track = await callback.message.edit_text(
            text=MessageTexts.NOT_FOUND,
            reply_markup=BotKeys.cancel()
        )
        return await tracker.add(track)
    
    # Create service list
    service_list = [(config["remark"], str(config["id"])) for config in configs]
    service_ids = [str(config["id"]) for config in configs]
    
    # Handle select all/deselect all
    if callback_data.select == SelectAll.SELECT:
        selected_services = service_ids.copy()
    elif callback_data.select == SelectAll.DESELECT:
        selected_services = []
    else:
        # Toggle individual selection
        if callback_data.select in selected_services:
            selected_services.remove(callback_data.select)
        else:
            selected_services.append(callback_data.select)
    
    await state.update_data(selected_services=selected_services)
    
    selected_admins = data.get("selected_admins", [])
    admins_text = ", ".join(selected_admins[:5])
    if len(selected_admins) > 5:
        admins_text += f" and {len(selected_admins) - 5} more"
    
    # Update the keyboard with new selection
    return await callback.message.edit_text(
        text=f"🔧 Select Services\n\nAdmins: {admins_text}\nSelected: {len(selected_services)}/{len(service_list)}\n\nChoose services to assign/remove:",
        reply_markup=BotKeys.selector(
            data=service_list,
            types=Pages.BULK_CONFIG,
            action=Actions.SELECT_SERVICE,
            panel=server.id,
            selects=selected_services,
            all_selects=True,
            server_back=server.id
        )
    )


@router.callback_query(
    StateFilter(BulkConfigForm.SELECT_SERVICES),
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.SELECT_SERVICE))
        & (F.done.is_(True))
    )
)
async def services_selected(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Show confirmation before processing"""
    data = await state.get_data()
    selected_services = data.get("selected_services", [])
    
    if not selected_services:
        await callback.answer("⚠️ Please select at least one service", show_alert=True)
        return
    
    selected_admins = data.get("selected_admins", [])
    action_type = data.get("action")
    configs = data.get("configs", [])
    
    # Get service names
    service_names = []
    for service_id in selected_services:
        config = next((c for c in configs if str(c["id"]) == service_id), None)
        if config:
            service_names.append(config["remark"])
    
    # Prepare confirmation message
    action_text = "ADD to" if action_type == ActionTypes.ADD_CONFIG.value else "REMOVE from"
    
    admins_text = ", ".join(selected_admins[:5])
    if len(selected_admins) > 5:
        admins_text += f" and {len(selected_admins) - 5} more"
    
    services_text = ", ".join(service_names[:5])
    if len(service_names) > 5:
        services_text += f" and {len(service_names) - 5} more"
    
    confirmation_text = (
        f"⚠️ Confirm Bulk Operation\n\n"
        f"Action: {action_text} users\n"
        f"Admins: {admins_text}\n"
        f"Services: {services_text}\n\n"
        f"This will affect all users of the selected admins.\n"
        f"Do you want to proceed?"
    )
    
    await state.set_state(BulkConfigForm.CONFIRM)
    
    return await callback.message.edit_text(
        text=confirmation_text,
        reply_markup=BotKeys.selector(
            data=["✅ Confirm", "❌ Cancel"],
            types=Pages.BULK_CONFIG,
            action=Actions.CONFIRM,
            panel=callback_data.panel,
            server_back=callback_data.panel
        )
    )


@router.callback_query(
    StateFilter(BulkConfigForm.CONFIRM),
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.CONFIRM))
    )
)
async def process_bulk_operation(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Process the bulk operation"""
    if callback_data.select == "❌ Cancel":
        track = await callback.message.edit_text(
            text="❌ Operation cancelled",
            reply_markup=BotKeys.cancel(server_back=callback_data.panel)
        )
        return await tracker.add(track)
    
    server = await crud.get_server(callback_data.panel)
    if not server:
        track = await callback.message.edit_text(
            text=MessageTexts.NOT_FOUND,
            reply_markup=BotKeys.cancel()
        )
        return await tracker.add(track)
    
    data = await state.get_data()
    selected_admins = data.get("selected_admins", [])
    selected_services = data.get("selected_services", [])
    action_type = data.get("action")
    configs = data.get("configs", [])
    
    # Convert service IDs to integers
    service_ids = [int(sid) for sid in selected_services]
    
    # Get service names for display
    service_names = []
    for service_id in selected_services:
        config = next((c for c in configs if str(c["id"]) == service_id), None)
        if config:
            service_names.append(config["remark"])
    
    await state.set_state(BulkConfigForm.PROCESSING)
    
    # Initial progress message
    progress_msg = await callback.message.edit_text(
        text="⏳ Processing Bulk Operation\n\nInitializing..."
    )
    
    # Progress callback for updates
    async def update_progress(admin: str, result: Dict[str, Any]):
        """Update progress message during processing"""
        try:
            progress_text = (
                f"⏳ Processing Bulk Operation\n\n"
                f"Current Admin: {admin}\n"
                f"Users Processed: {result['total_users']}\n"
                f"Operations: {result['operations']}\n"
                f"✅ Successful: {result['successful']}\n"
                f"⏭️ Skipped: {result['skipped']}\n"
                f"❌ Failed: {result['failed']}"
            )
            await progress_msg.edit_text(text=progress_text)
        except Exception as e:
            logger.warning(f"Could not update progress: {e}")
    
    # Process the bulk operation
    try:
        results = await bulk_manager.process_bulk_assignment(
            server=server,
            admins=selected_admins,
            service_ids=service_ids,
            action_type=action_type,
            progress_callback=update_progress
        )
        
        # Prepare result message
        action_text = "Added" if action_type == ActionTypes.ADD_CONFIG.value else "Removed"
        
        admins_text = ", ".join(selected_admins[:3])
        if len(selected_admins) > 3:
            admins_text += f" and {len(selected_admins) - 3} more"
        
        services_text = ", ".join(service_names[:3])
        if len(service_names) > 3:
            services_text += f" and {len(service_names) - 3} more"
        
        result_text = (
            f"✅ Bulk Operation Completed!\n\n"
            f"Action: {action_text} services\n"
            f"Admins: {admins_text}\n"
            f"Services: {services_text}\n\n"
            f"Results:\n"
            f"Total Users: {results['total_users']}\n"
            f"Total Operations: {results['total_operations']}\n"
            f"✅ Successful: {results['successful']}\n"
            f"⏭️ Skipped: {results['skipped']}\n"
            f"❌ Failed: {results['failed']}\n\n"
            f"Note: Operations count only includes actual API calls (successful + failed), not skipped items."
        )
        
        if results['errors']:
            error_sample = results['errors'][:3]
            result_text += f"\n\nSample Errors:\n"
            for error in error_sample:
                result_text += f"• {error[:50]}...\n"
        
        track = await callback.message.edit_text(
            text=result_text,
            reply_markup=BotKeys.cancel(server_back=server.id)
        )
        
    except Exception as e:
        logger.error(f"Bulk operation failed: {e}")
        track = await callback.message.edit_text(
            text=f"❌ Operation Failed\n\nError: {str(e)}",
            reply_markup=BotKeys.cancel(server_back=server.id)
        )
    
    await state.clear()
    return await tracker.add(track)
