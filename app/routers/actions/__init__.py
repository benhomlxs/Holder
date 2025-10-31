from aiogram import Router

from . import menu
from .items import configs, users, admin, bulk_configs

__all__ = ["setup_action_routers", "menu", "configs", "users", "admin", "bulk_configs"]


def setup_action_routers() -> Router:
    router = Router()

    router.include_router(menu.router)
    router.include_router(bulk_configs.router)  # Move bulk_configs before configs
    router.include_router(configs.router)
    router.include_router(users.router)
    router.include_router(admin.router)

    return router
