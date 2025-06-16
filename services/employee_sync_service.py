import asyncio
import logging
from typing import Generator

from clients.meta_client import MetaContrataClient
from utils.string_cleaning import normalize_full_name
from zk_cvs_client.apis import CVSecurityPersonAPI
from zk_cvs_client.models import Person

logger = logging.getLogger(__name__)

def generate_batch(collection: list, batch_size: int) -> Generator[list, None, None]:
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

    if len(new_codes) > 0:
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
        current_allowed = not cv_person.get("isDisabled", "0") or False

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
