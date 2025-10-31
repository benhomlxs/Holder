"""
Helper functions for config actions
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from app.api.types.marzneshin import MarzneshinUserResponse, UserExpireStrategy

logger = logging.getLogger(__name__)


def prepare_user_modify_data(user: MarzneshinUserResponse, preserve_all: bool = True) -> Dict[str, Any]:
    """
    Prepare user data for modification while preserving important fields
    
    Args:
        user: The user object to prepare data from
        preserve_all: Whether to preserve all fields or just required ones
    
    Returns:
        Dictionary with user data ready for API call
    """
    modify_data = {
        "username": user.username,
        "service_ids": user.service_ids,
    }
    
    # Always preserve expire strategy related fields
    if user.expire_strategy == UserExpireStrategy.START_ON_FIRST_USE:
        if user.usage_duration is not None:
            modify_data["usage_duration"] = user.usage_duration
        else:
            # Set default 1 day if it's None to prevent panel crash
            logger.warning(f"User {user.username} has START_ON_FIRST_USE but no usage_duration, setting to 1 day")
            modify_data["usage_duration"] = 86400  # 1 day in seconds
    
    elif user.expire_strategy == UserExpireStrategy.FIXED_DATE:
        if user.expire_date:
            modify_data["expire_date"] = user.expire_date.isoformat()
    
    # Preserve data limit fields
    if user.data_limit is not None:
        modify_data["data_limit"] = user.data_limit
    
    if user.data_limit_reset_strategy:
        modify_data["data_limit_reset_strategy"] = user.data_limit_reset_strategy.value
    
    # Preserve other fields if requested
    if preserve_all:
        if user.note is not None:
            modify_data["note"] = user.note
        
        if user.activation_deadline:
            modify_data["activation_deadline"] = user.activation_deadline.isoformat()
        
        # Preserve enabled status
        modify_data["enabled"] = user.enabled
        
        # Preserve expire strategy
        modify_data["expire_strategy"] = user.expire_strategy.value
    
    return modify_data


def validate_user_data(user: MarzneshinUserResponse) -> Optional[str]:
    """
    Validate user data before modification
    
    Args:
        user: The user object to validate
    
    Returns:
        Error message if validation fails, None if valid
    """
    if user.expire_strategy == UserExpireStrategy.START_ON_FIRST_USE:
        if user.usage_duration is None:
            return f"User {user.username} has START_ON_FIRST_USE but no usage_duration"
    
    elif user.expire_strategy == UserExpireStrategy.FIXED_DATE:
        if user.expire_date is None:
            return f"User {user.username} has FIXED_DATE but no expire_date"
    
    return None


def log_user_modification(username: str, action: str, service_id: int, success: bool, error: Optional[str] = None):
    """
    Log user modification action
    
    Args:
        username: Username being modified
        action: Action performed (ADD_CONFIG or DELETE_CONFIG)
        service_id: Service ID being added/removed
        success: Whether the operation was successful
        error: Error message if failed
    """
    if success:
        logger.info(f"Successfully {action} service {service_id} for user {username}")
    else:
        logger.error(f"Failed to {action} service {service_id} for user {username}: {error}")
