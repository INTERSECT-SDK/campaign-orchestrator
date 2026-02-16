"""API Router Definitions

This API is generally meant to be accessed by SDK Services and Clients, not people.
"""

from fastapi import APIRouter

from .v1 import router as v1_router

router = APIRouter()
router.include_router(v1_router)
