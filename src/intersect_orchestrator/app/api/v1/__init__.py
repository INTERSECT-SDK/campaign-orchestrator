from fastapi import APIRouter

from .endpoints import general
from .endpoints.orchestrator import routes as orchestrator_routes

router = APIRouter(prefix='/v1', tags=['V1'])
router.include_router(general.router)
router.include_router(orchestrator_routes.router, prefix='/orchestrator', tags=['Orchestrator'])
