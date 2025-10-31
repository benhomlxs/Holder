from enum import Enum


class Pages(str, Enum):
    HOME = "home"
    MENU = "menu"
    USERS = "users"
    SERVERS = "servers"
    ACTIONS = "actions"
    STATS = "stats"
    TEMPLATES = "templates"
    UPDATE = "update"
    BULK_CONFIG = "bulk_config"


class Actions(str, Enum):
    LIST = "list"
    INFO = "info"
    CREATE = "create"
    MODIFY = "modify"
    SEARCH = "search"
    JSON = "json"
    SELECT_ADMIN = "select_admin"
    SELECT_SERVICE = "select_service"
    CONFIRM = "confirm"


class YesOrNot(str, Enum):
    YES_USAGE = "YES_USAGE"
    YES_NORMAL = "YES_NORMAL"
    YES_CHARGE = "YES_CHARGE"
    YES = "✅ Yes"
    NO = "❌ No"


class SelectAll(str, Enum):
    SELECT = "select"
    DESELECT = "deselect"


class JsonHandler(str, Enum):
    USER = "user"


class RandomHandler(str, Enum):
    USERNAME = "username"
