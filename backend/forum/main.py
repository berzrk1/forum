import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as redis

from forum.api import api_router
from forum.auth import utils
from forum.config import settings
from forum.database.core import sessionlocal
from forum.cache.core import get_cache_pool

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Starting Forum API")

    app.state.cache = redis.Redis(connection_pool=get_cache_pool())
    async with sessionlocal() as session:
        await utils.init_roles(session)

    # before
    yield
    # after

    await app.state.cache.close()
    log.info("Forum API stopped")


app = FastAPI(lifespan=lifespan)
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if settings.is_development else settings.CORS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/healthcheck")
def healthcheck():
    return {"status": "ok", "environment": settings.ENVIRONMENT}
