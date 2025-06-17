import asyncio
import logging
from config import load_config
from clients.meta_client import get_meta_client
from clients.cv_client import get_cv_client
from services.employee_sync_service import sync_employees
from services.department_sync_service import sync_departments
from zk_cvs_client.apis import CVSecurityDepartmentAPI, CVSecurityPersonAPI
from logging_config import setup_logging

async def main():
    config = load_config()
    meta_cfg = config["metacontrata"]
    cv_cfg = config["cvsecurity"]

    async with get_meta_client(meta_cfg) as meta_client, get_cv_client(cv_cfg) as cv_client:
        cv_dept_api = CVSecurityDepartmentAPI(cv_client)
        cv_person_api = CVSecurityPersonAPI(cv_client)

        await sync_departments(meta_client, cv_dept_api)
        await sync_employees(meta_client, cv_person_api)

if __name__ == "__main__":
    setup_logging()
    asyncio.run(main())
