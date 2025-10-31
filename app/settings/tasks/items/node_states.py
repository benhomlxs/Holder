from typing import Dict, Tuple

# Global dictionary to track node states
# Format: {(server_remark, node_address): is_error}
node_states: Dict[Tuple[str, str], bool] = {}

def get_node_state(server_remark: str, node_address: str) -> bool:
    """Returns True if node was previously in error state, False otherwise"""
    return node_states.get((server_remark, node_address), False)

def set_node_state(server_remark: str, node_address: str, is_error: bool):
    """Updates node state and returns True if state changed"""
    key = (server_remark, node_address)
    old_state = node_states.get(key, None)
    node_states[key] = is_error
    # Return True if state changed (including first time seeing this node)
    return old_state != is_error
