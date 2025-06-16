import asyncio
import logging
import tomllib
from metacontrata_client import MetaContrataClient

from zk_cvs_client.models import Department
from zk_cvs_client.models import Person
from zk_cvs_client.apis import CVSecurityDepartmentAPI
from zk_cvs_client.apis import CVSecurityPersonAPI

from zk_cvs_client import CVSecurityAuth
from zk_cvs_client import CVSecurityClient

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Any, Generator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import re


def clean_string(text: str, mode: str) -> str:
    if mode == 'alphanumeric':
        return re.sub(r'[^a-zA-Z0-9\s]', '', text).strip()
    if mode == 'alphabetic':
        return re.sub(r'[^a-zA-Z\s]', '', text).strip()
    raise ValueError(f"Unknown mode: {mode}")


def normalize_full_name(name: str, last_name: str) -> tuple[str, str]:
    # CVSecurity has an unknown first/last name character limit
    # Using 25 per field at the moment
    # This limit doesn't seem to exist when using the WebUI
    clean_firstname = clean_string(name, 'alphabetic')
    clean_lastname = clean_string(last_name, 'alphabetic')
    return clean_firstname[:25], clean_lastname[:25]


@asynccontextmanager
async def get_meta_client(meta_cfg: dict) -> AsyncGenerator[MetaContrataClient, None]:
    async with MetaContrataClient(
            meta_cfg["username"],
            meta_cfg["password"],
            meta_cfg["base_url"]
    ) as client:
        await client.authenticate()
        yield client


@asynccontextmanager
async def get_cv_client(cv_cfg: dict) -> AsyncGenerator[CVSecurityClient, None]:
    auth = CVSecurityAuth(
        server_host=cv_cfg["base_url"],
        server_port=cv_cfg["port"],
        token=cv_cfg["api_key"]
    )
    async with CVSecurityClient(auth, ignore_ssl=cv_cfg["ignore_ssl"]) as client:
        yield client


def load_config(path="config.toml") -> dict:
    with open(path, 'rb') as file:
        return tomllib.load(file)


def compare_employees(meta_emp: dict, cv_person: dict) -> bool:
    if meta_emp['coInEmpl'] != cv_person['pin']:
        raise Exception("Can only compare employees with same pin/code")
    if meta_emp['nombre'] != cv_person['name']:
        return False
    if f"{meta_emp['apellido1']} {meta_emp['apellido2']}" != cv_person['lastName']:
        return False
    return True


async def sync_departments(meta_client: MetaContrataClient, cv_departments_api: CVSecurityDepartmentAPI):
    async with asyncio.TaskGroup() as tg:
        logger.info("Retrieving CVSecurity departments...")
        cv_departments_task = tg.create_task(cv_departments_api.get_all_departments())
        logger.info("Retrieving MetaContrata subcontratas...")
        meta_subcontratas_task = tg.create_task(meta_client.get_subcontrata_list())

    meta_subcontratas = meta_subcontratas_task.result()
    cv_departments = cv_departments_task.result()

    meta_subc_codes: set[str] = set([str(subc['coInSub']) for subc in meta_subcontratas])
    cv_dept_codes: set[str] = set([str(dept['code']) for dept in cv_departments if dept['code'] != '1'])

    new_departments = [subc for subc in meta_subcontratas if subc['coInSub'] not in cv_dept_codes]
    logging.info(f"Starting the creation of {len(new_departments)} departments")
    for dept in new_departments:
        # CV Security doesn't allow repeated department names, so the {Code - Name} nomenclature will be used
        new_dept = Department(code=str(dept['coInSub']),
                              name=f"{dept['coInSub']}-{clean_string(dept['nombre'], 'alphanumeric')}")
        try:
            await cv_departments_api.add_or_edit_department(new_dept)
        except Exception as e:
            logger.error(f"Error creating department/subcontrata: {new_dept.model_dump_json()}", exc_info=e)

    # CVSecurity has a default department code '1'. This must be excluded
    delete_dept_codes = [dept_code for dept_code in cv_dept_codes if
                         dept_code not in meta_subc_codes and dept_code != '1']
    logging.info(f"Starting the deletion of {delete_dept_codes}")
    for code in delete_dept_codes:
        try:
            await cv_departments_api.delete_department(code)
        except Exception as e:
            logger.error(f"Error deleting department/subcontrata code: {code}", exc_info=e)

    return


def generate_batch(collection: list, batch_size: int):
    if batch_size <= 0:
        return
    for i in range(0, len(collection), batch_size):
        yield collection[i:i + batch_size]


async def sync_employees(meta_client: MetaContrataClient, cv_employees_api: CVSecurityPersonAPI):
    async with asyncio.TaskGroup() as tg:
        logger.info("Retrieving CVSecurity employees...")
        cv_employees_task = tg.create_task(cv_employees_api.get_all_persons())
        logger.info("Retrieving all MetaContrata employees...")
        meta_employees_task = tg.create_task(meta_client.get_employee_list())

    meta_employees = meta_employees_task.result()
    cv_employees = cv_employees_task.result()

    meta_by_code = {str(emp["coInEmpl"]): emp for emp in meta_employees}
    cv_by_code = {str(emp["pin"]): emp for emp in cv_employees}

    meta_emp_codes = set(meta_by_code.keys())
    cv_emp_codes = set(cv_by_code.keys())

    # Determine new, deleted, and common employees
    new_codes = meta_emp_codes - cv_emp_codes
    deleted_codes = cv_emp_codes - meta_emp_codes
    common_codes = meta_emp_codes & cv_emp_codes

    # --- CREATE ---
    to_create: list[Person] = []

    if len(to_create) > 0:
        for code in new_codes:
            emp = meta_by_code[code]
            first_name, last_name = normalize_full_name(emp["nombre"], f'{emp["apellido1"]} {emp["apellido2"]}')
            to_create.append(Person(
                pin=code,
                deptCode=str(emp["coInSub"]),
                name=first_name,
                lastName=last_name
            ))

        logger.info(f"Creating {len(to_create)} new employees")
        for i, batch in enumerate(generate_batch(to_create, 100)):
            logger.info(f"Create batch {i + 1}")
            await cv_employees_api.bulk_add_or_edit_persons(batch)

        # Update after creation
        cv_employees = await cv_employees_api.get_all_persons()
        cv_by_code = {str(emp["pin"]): emp for emp in cv_employees}
        cv_emp_codes = set(cv_by_code.keys())
        common_codes = meta_emp_codes & cv_emp_codes

    # --- UPDATE ---
    to_update: list[Person] = []
    for code in common_codes:
        meta_emp = meta_by_code[code]
        cv_person = cv_by_code[code]

        first_name, last_name = normalize_full_name(meta_emp["nombre"],
                                                    f'{meta_emp["apellido1"]} {meta_emp["apellido2"]}')
        access_allowed = bool(int(meta_emp.get("accesoPermitido", "0")))
        current_allowed = True if None == cv_person.get("isDisabled", "0") or "0" else False

        if (
                first_name != cv_person.get("name") or
                last_name != cv_person.get("lastName") or
                access_allowed != current_allowed
        ):
            to_update.append(Person(
                pin=code,
                deptCode=str(meta_emp["coInSub"]),
                name=first_name,
                lastName=last_name,
                isDisabled=not access_allowed
            ))

    if len(to_update) > 0:
        logger.info(f"Updating {len(to_update)} existing employees")
        for i, batch in enumerate(generate_batch(to_update, 100)):
            logger.info(f"Update batch {i + 1}")
            await cv_employees_api.bulk_add_or_edit_persons(batch)

    # --- DELETE ---
    if len(deleted_codes) > 0:
        to_delete: list[str] = list(deleted_codes)

        logger.info(f"Deleting {len(to_delete)} obsolete employees")
        for i, batch in enumerate(generate_batch(to_delete, 100)):
            logger.info(f"Delete batch {i + 1}")
            await cv_employees_api.bulk_delete_persons_by_pin(batch)


async def main() -> None:
    config = load_config()
    meta_cfg = config["metacontrata"]
    cv_cfg = config["cvsecurity"]

    async with get_meta_client(meta_cfg) as meta_client, get_cv_client(cv_cfg) as cv_client:
        cv_department_api = CVSecurityDepartmentAPI(cv_client)
        cv_person_api = CVSecurityPersonAPI(cv_client)
        await sync_employees(meta_client, cv_person_api)
        # await sync_departments(meta_client, cv_department_api)


if __name__ == "__main__":
    asyncio.run(main())
