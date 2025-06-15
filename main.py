import asyncio
import logging
import tomllib
from metacontrata_client import MetaContrataClient
from src.zk_cvs_client.apis.departmentApi import CVSecurityDepartmentAPI

from src.zk_cvs_client.apis.personApi import CVSecurityPersonAPI
from src.zk_cvs_client.auth import CVSecurityAuth
from src.zk_cvs_client.client import CVSecurityClient

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Any, Generator

from src.zk_cvs_client.models.department import Department
from src.zk_cvs_client.models.person import Person

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

    meta_emp_codes: set[str] = {str(emp['coInEmpl']) for emp in meta_employees}
    cv_emp_codes: set[str] = {str(emp['pin']) for emp in cv_employees}

    new_employees = [emp for emp in meta_employees if str(emp['coInEmpl']) not in cv_emp_codes]
    new_cv_employees = []
    for emp in new_employees:
        first_name, last_name = normalize_full_name(emp['nombre'], emp['apellido1'] + ' ' + emp['apellido2'])
        new_cv_employees.append(Person(pin=str(emp['coInEmpl']),
                                       deptCode=str(emp['coInSub']),
                                       name=first_name,
                                       lastName=last_name))

    # cv_create_emp_task = asyncio.create_task(cv_employees_api.bulk_add_or_edit_persons(new_cv_employees))
    logging.info(f"Starting the creation of {len(new_employees)} employees")
    for i, emp_batch in enumerate(generate_batch(new_cv_employees, 100)):
        logging.info(f"Batch {i + 1} / {int(len(new_cv_employees) / 100)}")
        await cv_employees_api.bulk_add_or_edit_persons(emp_batch)

        del_employees: list[str] = [str(emp['pin']) for emp in cv_employees if emp['pin'] not in meta_emp_codes]

    cv_delete_emp_task = cv_employees_api.bulk_delete_persons_by_pin(del_employees)

    # await cv_create_emp_task
    await cv_delete_emp_task
    # for emp in meta_employees:
    #     if emp["coInEmpl"] not in cv_emp_codes:
    #         new_persons.append(Person(
    #             pin = code,
    #             first emp["nombre"],
    #             "lastName": f"{emp['apellido1']} {emp['apellido2']}".strip(),
    #             "isDisabled": emp["accesoPermitido"] == 0
    #         ))

    # Handle deletions
    # for code in cv_emp_codes - meta_emp_codes:
    #     cv_client.delete_employee(code)
    #
    # # Handle updates
    # for emp in meta_employees:
    #     code = emp["coInEmpl"]
    #     if code in cv_emp_codes:
    #         existing = cv_employees_code[code]
    #         full_last_name = f"{emp['apellido1']} {emp['apellido2']}".strip()
    #         need_update = (
    #                 normalize_name(existing["firstName"]) != normalize_name(emp["nombre"]) or
    #                 normalize_name(existing["lastName"]) != normalize_name(full_last_name)
    #         )
    #         if need_update:
    #             cv_client.update_employee({
    #                 "employeeCode": code,
    #                 "firstName": emp["nombre"],
    #                 "lastName": full_last_name,
    #             })
    #
    #         access_allowed = emp["accesoPermitido"] == 1
    #         if existing["accessEnabled"] != access_allowed:
    #             cv_client.set_access(code, access_allowed)


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
