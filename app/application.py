import asyncio
from functools import partial

from app.helpers.api import Server, add_health_check_router, add_object_not_found_handler
from app.helpers.metrics import MetricsMiddleware, add_prometheus_extension
from app.helpers.optimization import ujson_enable
from starlette.middleware.base import BaseHTTPMiddleware

from app.config import settings
from app.container import Container
from app.workers.imitator_transactions import ImitatorTransactions


async def emulate_data(imitator_transactions: ImitatorTransactions = Container.imitator_transactions()):
    while True:
        await imitator_transactions.emulate()
        await asyncio.sleep(10)


async def start_callbacks():
    asyncio.create_task(emulate_data())


ujson_enable()
app = Server(
    name=settings.NAME,
    version=settings.VERSION,
    description="Imitator service",
    logging_config=settings.LOGGING,
    cors_config=settings.CORS,
    routers=[],
    middlewares=[MetricsMiddleware(BaseHTTPMiddleware)],
    start_callbacks=[start_callbacks],
    stop_callbacks=[Container.redis().close],
    exception_handlers=[add_object_not_found_handler],
    extensions=[
        partial(
            add_health_check_router,
            service=settings.NAME,
            version=settings.VERSION,
            branch=settings.BRANCH,
            commit=settings.COMMIT,
        ),
        add_prometheus_extension,
    ],
).app
