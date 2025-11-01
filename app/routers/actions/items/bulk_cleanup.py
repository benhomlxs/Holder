"""
Bulk user cleanup module for managing user deletion based on status
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
from app.models.server import ServerTypes
from app.api import ClinetManager
from app.api.types.marzneshin import MarzneshinUserResponse
from app.api.types.marzban import MarzbanUserResponse, MarzbanUserStatus

router = Router(name="bulk_cleanup")
logger = logging.getLogger(__name__)

# Configure logging to see debug messages
logging.basicConfig(level=logging.INFO)


class BulkCleanupForm(StatesGroup):
    """States for bulk cleanup workflow"""
    SELECT_ADMINS = State()
    SELECT_STATUS = State()
    CONFIRM = State()
    PROCESSING = State()


class CircuitBreaker:
    """Simple circuit breaker to prevent server overload"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 30):
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


class BulkCleanupManager:
    """Manager for bulk user cleanup operations with optimized batch processing"""
    
    def __init__(self, batch_size: int = 8, concurrent_limit: int = 2, rate_limit_delay: float = 0.2):
        self.batch_size = batch_size
        self.concurrent_limit = concurrent_limit
        self.rate_limit_delay = rate_limit_delay
        self.progress_updates = {}
        self.circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30)
        
    async def process_bulk_cleanup(
        self,
        server,
        admins: List[str],
        status_filters: List[str],
        progress_callback=None
    ) -> Dict[str, Any]:
        """
        Process bulk user cleanup for multiple admins based on status filters
        
        Args:
            server: Server object
            admins: List of admin usernames to process
            status_filters: List of status filters to match for deletion
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dictionary with operation results
        """
        results = {
            "total_users": 0,
            "total_deleted": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "errors": [],
            "admin_results": {},
            "processed_users": set()
        }
        
        for admin in admins:
            admin_result = await self._process_admin_cleanup(
                server, admin, status_filters, progress_callback, results["processed_users"]
            )
            results["admin_results"][admin] = admin_result
            results["total_users"] = len(results["processed_users"])
            results["total_deleted"] += admin_result["deleted"]
            results["successful"] += admin_result["successful"]
            results["failed"] += admin_result["failed"]
            results["skipped"] += admin_result["skipped"]
            if admin_result["errors"]:
                results["errors"].extend(admin_result["errors"])
        
        # Remove the set from final results
        del results["processed_users"]
        return results
    
    async def _process_admin_cleanup(
        self,
        server,
        admin: str,
        status_filters: List[str],
        progress_callback=None,
        processed_users_set=None
    ) -> Dict[str, Any]:
        """Process cleanup for all users of a single admin"""
        result = {
            "total_users": 0,
            "deleted": 0,
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
            
            # Filter users that match status criteria and haven't been processed
            users_to_delete = []
            for user in users:
                if user.username not in processed_users_set:
                    processed_users_set.add(user.username)
                    admin_users_count += 1
                    
                    if self._should_delete_user(user, status_filters, server.types):
                        users_to_delete.append(user)
            
            # Process users in batches
            for i in range(0, len(users_to_delete), self.batch_size):
                batch = users_to_delete[i:i+self.batch_size]
                if not batch:
                    continue
                
                batch_results = await self._process_user_batch(server, batch)
                
                result["deleted"] += batch_results["deleted"]
                result["successful"] += batch_results["successful"]
                result["failed"] += batch_results["failed"]
                result["skipped"] += batch_results["skipped"]
                if batch_results["errors"]:
                    result["errors"].extend(batch_results["errors"])
                
                # Send progress update if callback provided
                if progress_callback:
                    temp_result = result.copy()
                    temp_result["total_users"] = len(processed_users_set)
                    await progress_callback(admin, temp_result)
                    
            page += 1
            
        result["total_users"] = admin_users_count
        return result
    
    def _should_delete_user(self, user, status_filters: List[str], server_type: str) -> bool:
        """Check if user should be deleted based on status filters"""
        if server_type == ServerTypes.MARZNESHIN.value:
            # Marzneshin status mapping
            user_statuses = []
            if not user.activated:
                user_statuses.append("inactive")
            if user.expired:
                user_statuses.append("expired")
            if user.data_limit_reached:
                user_statuses.append("limited")
            if not user.enabled:
                user_statuses.append("disabled")
            if not user.is_active:
                user_statuses.append("not_active")
                
        else:  # Marzban
            # Marzban status mapping
            user_statuses = [user.status.value] if hasattr(user.status, 'value') else [str(user.status)]
            
        # Check if any of the user's statuses match the filter
        return any(status in status_filters for status in user_statuses)
    
    async def _process_user_batch(
        self,
        server,
        users: List,
        
    ) -> Dict[str, Any]:
        """Process a batch of users for deletion"""
        result = {
            "deleted": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "errors": []
        }
        
        # Create tasks for user deletion
        tasks = []
        for user in users:
            task = self._delete_single_user(server, user)
            tasks.append(task)
        
        if not tasks:
            return result
        
        # Process with concurrency limit
        semaphore = asyncio.Semaphore(self.concurrent_limit)
        
        async def limited_task(task):
            async with semaphore:
                try:
                    return await task
                except Exception as e:
                    logger.error(f"Task failed with exception: {e}")
                    return "failed"
        
        # Execute tasks
        results = await asyncio.gather(
            *(limited_task(task) for task in tasks),
            return_exceptions=False
        )
        
        # Count results
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                result["failed"] += 1
                result["errors"].append(str(res))
            elif res == "success":
                result["deleted"] += 1
                result["successful"] += 1
            elif res == "failed":
                result["failed"] += 1
            elif res == "skipped":
                result["skipped"] += 1
                
        return result
    
    async def _delete_single_user(self, server, user) -> str:
        """Delete a single user"""
        try:
            # Check circuit breaker
            if not self.circuit_breaker.can_execute():
                logger.warning(f"Circuit breaker is open, skipping {user.username}")
                return "failed"
            
            # Add rate limiting delay
            await asyncio.sleep(self.rate_limit_delay)
            
            # Delete user using API
            result = await ClinetManager.remove_user(
                server=server,
                username=user.username
            )
            
            # Update circuit breaker based on result
            if result:
                self.circuit_breaker.record_success()
                logger.info(f"Successfully deleted user: {user.username}")
                return "success"
            else:
                self.circuit_breaker.record_failure()
                logger.warning(f"Failed to delete user: {user.username}")
                return "failed"
                
        except Exception as e:
            logger.error(f"Error deleting user {user.username}: {e}")
            self.circuit_breaker.record_failure()
            return "failed"


# Initialize the bulk cleanup manager with optimized settings
cleanup_manager = BulkCleanupManager(
    batch_size=8,  # Conservative batch size to prevent overload
    concurrent_limit=2,  # Low concurrency for stability
    rate_limit_delay=0.2  # Rate limiting to prevent server stress
)


def get_status_options(server_type: str) -> List[tuple]:
    """Get available status options based on server type"""
    if server_type == ServerTypes.MARZNESHIN.value:
        return [
            ("🔴 Inactive (Not Activated)", "inactive"),
            ("⏰ Expired", "expired"),
            ("📊 Limited (Data Limit Reached)", "limited"),
            ("❌ Disabled", "disabled"),
            ("💤 Not Active", "not_active"),
        ]
    else:  # Marzban
        return [
            ("❌ Disabled", "disabled"),
            ("📊 Limited", "limited"),
            ("⏰ Expired", "expired"),
            ("⏸️ On Hold", "on_hold"),
        ]


@router.callback_query(
    SelectCB.filter(
        (F.types == Pages.ACTIONS)
        & (F.action == Actions.INFO)
        & (F.select == "Bulk User Cleanup")
    )
)
async def start_bulk_cleanup(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Start bulk user cleanup workflow"""
    logger.info(f"Bulk cleanup handler triggered - callback_data: {callback_data}")
    
    server = await crud.get_server(callback_data.panel)
    if not server:
        track = await callback.message.edit_text(
            text=MessageTexts.NOT_FOUND,
            reply_markup=BotKeys.cancel()
        )
        return await tracker.add(track)
    
    await state.set_state(BulkCleanupForm.SELECT_ADMINS)
    await state.update_data(selected_admins=[], server_type=server.types)
    
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
        text="🧹 Bulk User Cleanup\n\n📋 Select Admins\n\nChoose one or more admins whose users will be cleaned up:",
        reply_markup=BotKeys.selector(
            data=admin_list,
            types=Pages.BULK_CONFIG,  # Reuse existing page type
            action=Actions.SELECT_ADMIN,
            panel=server.id,
            selects=[],
            all_selects=True,
            server_back=server.id
        )
    )


@router.callback_query(
    StateFilter(BulkCleanupForm.SELECT_ADMINS),
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.SELECT_ADMIN))
        & (F.done.is_not(True))
    )
)
async def toggle_admin_selection_cleanup(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Handle admin checkbox toggle for cleanup"""
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
        text=f"🧹 Bulk User Cleanup\n\n📋 Select Admins\n\nSelected: {len(selected_admins)}/{len(admin_list)}\n\nChoose admins whose users will be cleaned up:",
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
    StateFilter(BulkCleanupForm.SELECT_ADMINS),
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.SELECT_ADMIN))
        & (F.done.is_(True))
    )
)
async def admins_selected_cleanup(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
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
    
    await state.set_state(BulkCleanupForm.SELECT_STATUS)
    await state.update_data(selected_statuses=[])
    
    # Get status options based on server type
    status_options = get_status_options(server_type)
    
    admins_text = ", ".join(selected_admins[:5])
    if len(selected_admins) > 5:
        admins_text += f" and {len(selected_admins) - 5} more"
    
    return await callback.message.edit_text(
        text=f"🧹 Bulk User Cleanup\n\n🏷️ Select Status Filters\n\nAdmins: {admins_text}\n\nChoose user statuses to delete:",
        reply_markup=BotKeys.selector(
            data=status_options,
            types=Pages.BULK_CONFIG,
            action=Actions.SELECT_SERVICE,  # Reuse existing action
            panel=server.id,
            selects=[],
            all_selects=True,
            server_back=server.id
        )
    )


@router.callback_query(
    StateFilter(BulkCleanupForm.SELECT_STATUS),
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.SELECT_SERVICE))
        & (F.done.is_not(True))
    )
)
async def toggle_status_selection(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Handle status checkbox toggle"""
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
        text=f"🧹 Bulk User Cleanup\n\n🏷️ Select Status Filters\n\nAdmins: {admins_text}\nSelected: {len(selected_statuses)}/{len(status_options)}\n\nChoose user statuses to delete:",
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
    StateFilter(BulkCleanupForm.SELECT_STATUS),
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.SELECT_SERVICE))
        & (F.done.is_(True))
    )
)
async def statuses_selected(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Show confirmation before processing cleanup"""
    data = await state.get_data()
    selected_statuses = data.get("selected_statuses", [])
    
    if not selected_statuses:
        await callback.answer("⚠️ Please select at least one status", show_alert=True)
        return
    
    selected_admins = data.get("selected_admins", [])
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
    
    confirmation_text = (
        f"⚠️ Confirm Bulk User Cleanup\n\n"
        f"🗑️ Action: DELETE users\n"
        f"👥 Admins: {admins_text}\n"
        f"🏷️ Status Filters: {statuses_text}\n\n"
        f"⚠️ WARNING: This will permanently delete all users matching the selected criteria!\n\n"
        f"Do you want to proceed?"
    )
    
    await state.set_state(BulkCleanupForm.CONFIRM)
    
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
    StateFilter(BulkCleanupForm.CONFIRM),
    SelectCB.filter(
        (F.types.is_(Pages.BULK_CONFIG))
        & (F.action.is_(Actions.CONFIRM))
    )
)
async def process_bulk_cleanup(callback: CallbackQuery, callback_data: SelectCB, state: FSMContext):
    """Process the bulk cleanup operation"""
    if callback_data.select == "❌ Cancel":
        track = await callback.message.edit_text(
            text="❌ Cleanup operation cancelled",
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
    selected_statuses = data.get("selected_statuses", [])
    server_type = data.get("server_type")
    
    await state.set_state(BulkCleanupForm.PROCESSING)
    
    # Initial progress message
    progress_msg = await callback.message.edit_text(
        text="🧹 Processing Bulk User Cleanup\n\nInitializing..."
    )
    
    # Progress callback for updates
    async def update_progress(admin: str, result: Dict[str, Any]):
        """Update progress message during processing"""
        try:
            progress_text = (
                f"🧹 Processing Bulk User Cleanup\n\n"
                f"Current Admin: {admin}\n"
                f"Users Processed: {result['total_users']}\n"
                f"🗑️ Deleted: {result['deleted']}\n"
                f"✅ Successful: {result['successful']}\n"
                f"⏭️ Skipped: {result['skipped']}\n"
                f"❌ Failed: {result['failed']}"
            )
            await progress_msg.edit_text(text=progress_text)
        except Exception as e:
            logger.warning(f"Could not update progress: {e}")
    
    # Process the bulk cleanup
    try:
        results = await cleanup_manager.process_bulk_cleanup(
            server=server,
            admins=selected_admins,
            status_filters=selected_statuses,
            progress_callback=update_progress
        )
        
        # Get status names for display
        status_options = get_status_options(server_type)
        status_names = []
        for status_value in selected_statuses:
            status_option = next((opt for opt in status_options if opt[1] == status_value), None)
            if status_option:
                status_names.append(status_option[0])
        
        # Prepare result message
        admins_text = ", ".join(selected_admins[:3])
        if len(selected_admins) > 3:
            admins_text += f" and {len(selected_admins) - 3} more"
        
        statuses_text = ", ".join(status_names[:3])
        if len(status_names) > 3:
            statuses_text += f" and {len(status_names) - 3} more"
        
        result_text = (
            f"✅ Bulk User Cleanup Completed!\n\n"
            f"👥 Admins: {admins_text}\n"
            f"🏷️ Status Filters: {statuses_text}\n\n"
            f"Results:\n"
            f"Total Users Processed: {results['total_users']}\n"
            f"🗑️ Total Deleted: {results['total_deleted']}\n"
            f"✅ Successful: {results['successful']}\n"
            f"⏭️ Skipped: {results['skipped']}\n"
            f"❌ Failed: {results['failed']}"
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
        logger.error(f"Bulk cleanup operation failed: {e}")
        track = await callback.message.edit_text(
            text=f"❌ Cleanup Operation Failed\n\nError: {str(e)}",
            reply_markup=BotKeys.cancel(server_back=server.id)
        )
    
    await state.clear()
    return await tracker.add(track)
