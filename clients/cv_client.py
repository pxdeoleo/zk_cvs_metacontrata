from contextlib import asynccontextmanager
from typing import AsyncGenerator
from zk_cvs_client import CVSecurityAuth, CVSecurityClient

@asynccontextmanager
async def get_cv_client(cfg: dict) -> AsyncGenerator[CVSecurityClient, None]:
    auth = CVSecurityAuth(
        server_host=cfg["base_url"],
        server_port=cfg["port"],
        token=cfg["api_key"]
    )
    async with CVSecurityClient(auth, ignore_ssl=cfg["ignore_ssl"]) as client:
        yield client
