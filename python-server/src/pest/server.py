from starlette.responses import Response, JSONResponse, StreamingResponse
from starlette.requests import Request
from starlette.applications import Starlette
from starlette.routing import Route
import logging
import orjson
from pest.db import Database
from pest.config import Config
from typing import AsyncIterator
from contextlib import asynccontextmanager
from typing import TypedDict

logger = logging.getLogger(__name__)

# lifespan
class State(TypedDict):
    db: Database

@asynccontextmanager
async def lifespan(app: Starlette) -> AsyncIterator[State]:
    db = Database(host=Config.dbhost(), root=Config.dbroot())
    yield {"db": db}
    await db.finalize()

# responses

class OrjsonResponse(JSONResponse):
    def render(self, content: dict) -> bytes:
        return orjson.dumps(content)

ok_response = Response()

# routes

async def status(request: Request) -> Response:
    return ok_response

async def save(request: Request) -> Response:
    # TODO optionally derive key, position, is_close from request
    db = request.state.db
    key = await db.newkey()
    position = 0
    is_close = True
    async for chunk in request.stream():
        position = await db.save(key, position, chunk, False)
    if is_close:
        await db.save(key, position, b'', True)
    return OrjsonResponse({'key': key})

async def read(request: Request) -> Response:
    db = request.state.db
    key: str = request.path_params['key']
    start: int = request.path_params['start']
    end: int = request.path_params['end']
    async def gen() -> AsyncIterator[bytes]:
        # NOTE there appears no yield from in async
        async for chunk in db.read(key, start, end):
            yield chunk
    return StreamingResponse(gen(), media_type='application/octet-stream')

app = Starlette(
    debug=Config.is_starlette_debug(), 
    routes = [
        Route('/status', status, methods=["GET", "HEAD"]),
        Route('/save', save, methods=["PUT"]),
        Route('/read/{key}/{start:int}/{end:int}', read, methods=["GET"]),
    ],
    lifespan=lifespan,
)
