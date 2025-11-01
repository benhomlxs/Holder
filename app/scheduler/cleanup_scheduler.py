"""
Automatic cleanup scheduler for periodic user cleanup operations
"""
import asyncio
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import json

from app.db import crud
from app.api import ClinetManager
from app.models.server import ServerTypes
from app.routers.actions.items.bulk_cleanup import BulkCleanupManager

logger = logging.getLogger(__name__)


@dataclass
class CleanupTask:
    """Data class for cleanup task configuration"""
    id: str
    server_id: int
    admin_usernames: List[str]
    status_filters: List[str]
    interval_hours: int
    enabled: bool = True
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.next_run is None:
            self.next_run = datetime.now() + timedelta(hours=self.interval_hours)
    
    def to_dict(self) -> dict:
        """Convert to dictionary for storage"""
        data = asdict(self)
        # Convert datetime objects to ISO strings
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat() if value else None
        return data
    
    @classmethod
    def from_dict(cls, data: dict) -> 'CleanupTask':
        """Create from dictionary"""
        # Convert ISO strings back to datetime objects
        for key in ['last_run', 'next_run', 'created_at']:
            if data.get(key):
                data[key] = datetime.fromisoformat(data[key])
        return cls(**data)


class CleanupScheduler:
    """Scheduler for automatic user cleanup operations"""
    
    def __init__(self):
        self.tasks: Dict[str, CleanupTask] = {}
        self.cleanup_manager = BulkCleanupManager(
            batch_size=5,  # Smaller batches for scheduled operations
            concurrent_limit=1,  # Very conservative for background operations
            rate_limit_delay=0.5  # Longer delays for background operations
        )
        self.running = False
        self._scheduler_task = None
        self.storage_file = "cleanup_tasks.json"
        
    async def start(self):
        """Start the scheduler"""
        if self.running:
            return
        
        self.running = True
        await self.load_tasks()
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        logger.info("Cleanup scheduler started")
    
    async def stop(self):
        """Stop the scheduler"""
        self.running = False
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
        await self.save_tasks()
        logger.info("Cleanup scheduler stopped")
    
    async def add_task(
        self,
        task_id: str,
        server_id: int,
        admin_usernames: List[str],
        status_filters: List[str],
        interval_hours: int
    ) -> bool:
        """Add a new cleanup task"""
        try:
            # Validate server exists
            server = await crud.get_server(server_id)
            if not server:
                logger.error(f"Server {server_id} not found")
                return False
            
            # Create task
            task = CleanupTask(
                id=task_id,
                server_id=server_id,
                admin_usernames=admin_usernames,
                status_filters=status_filters,
                interval_hours=interval_hours
            )
            
            self.tasks[task_id] = task
            await self.save_tasks()
            
            logger.info(f"Added cleanup task {task_id} for server {server_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add cleanup task {task_id}: {e}")
            return False
    
    async def remove_task(self, task_id: str) -> bool:
        """Remove a cleanup task"""
        if task_id in self.tasks:
            del self.tasks[task_id]
            await self.save_tasks()
            logger.info(f"Removed cleanup task {task_id}")
            return True
        return False
    
    async def enable_task(self, task_id: str) -> bool:
        """Enable a cleanup task"""
        if task_id in self.tasks:
            self.tasks[task_id].enabled = True
            await self.save_tasks()
            logger.info(f"Enabled cleanup task {task_id}")
            return True
        return False
    
    async def disable_task(self, task_id: str) -> bool:
        """Disable a cleanup task"""
        if task_id in self.tasks:
            self.tasks[task_id].enabled = False
            await self.save_tasks()
            logger.info(f"Disabled cleanup task {task_id}")
            return True
        return False
    
    async def get_tasks(self) -> Dict[str, CleanupTask]:
        """Get all tasks"""
        return self.tasks.copy()
    
    async def get_task(self, task_id: str) -> Optional[CleanupTask]:
        """Get a specific task"""
        return self.tasks.get(task_id)
    
    async def _scheduler_loop(self):
        """Main scheduler loop"""
        while self.running:
            try:
                now = datetime.now()
                
                # Check each task
                for task_id, task in list(self.tasks.items()):
                    if not task.enabled:
                        continue
                    
                    if task.next_run and now >= task.next_run:
                        await self._execute_task(task)
                
                # Sleep for 1 minute before next check
                await asyncio.sleep(60)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                await asyncio.sleep(60)
    
    async def _execute_task(self, task: CleanupTask):
        """Execute a cleanup task"""
        try:
            logger.info(f"Executing cleanup task {task.id}")
            
            # Get server
            server = await crud.get_server(task.server_id)
            if not server:
                logger.error(f"Server {task.server_id} not found for task {task.id}")
                return
            
            # Execute cleanup
            results = await self.cleanup_manager.process_bulk_cleanup(
                server=server,
                admins=task.admin_usernames,
                status_filters=task.status_filters
            )
            
            # Update task timing
            task.last_run = datetime.now()
            task.next_run = task.last_run + timedelta(hours=task.interval_hours)
            
            # Log results
            logger.info(
                f"Cleanup task {task.id} completed: "
                f"Users processed: {results['total_users']}, "
                f"Deleted: {results['total_deleted']}, "
                f"Failed: {results['failed']}"
            )
            
            # Save updated task
            await self.save_tasks()
            
        except Exception as e:
            logger.error(f"Failed to execute cleanup task {task.id}: {e}")
            # Still update next run time to prevent continuous retries
            task.next_run = datetime.now() + timedelta(hours=task.interval_hours)
            await self.save_tasks()
    
    async def save_tasks(self):
        """Save tasks to file"""
        try:
            data = {task_id: task.to_dict() for task_id, task in self.tasks.items()}
            with open(self.storage_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to save tasks: {e}")
    
    async def load_tasks(self):
        """Load tasks from file"""
        try:
            with open(self.storage_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.tasks = {}
            for task_id, task_data in data.items():
                try:
                    self.tasks[task_id] = CleanupTask.from_dict(task_data)
                except Exception as e:
                    logger.error(f"Failed to load task {task_id}: {e}")
            
            logger.info(f"Loaded {len(self.tasks)} cleanup tasks")
            
        except FileNotFoundError:
            logger.info("No existing tasks file found, starting with empty tasks")
        except Exception as e:
            logger.error(f"Failed to load tasks: {e}")


# Global scheduler instance
cleanup_scheduler = CleanupScheduler()
