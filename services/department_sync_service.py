import asyncio
import logging
from zk_cvs_client.apis import CVSecurityDepartmentAPI
from zk_cvs_client.models import Department

from clients.meta_client import MetaContrataClient
from utils.string_cleaning import clean_string

logger = logging.getLogger(__name__)


async def sync_departments(meta_client: MetaContrataClient, cv_departments_api: CVSecurityDepartmentAPI):
    async with asyncio.TaskGroup() as tg:
        logger.info("Retrieving CVSecurity departments...")
        cv_departments_task = tg.create_task(cv_departments_api.get_all_departments())
        logger.info("Retrieving MetaContrata subcontratas...")
        meta_subcontratas_task = tg.create_task(meta_client.get_subcontrata_list())

    meta_subcontratas = meta_subcontratas_task.result()
    cv_departments = cv_departments_task.result()

    meta_by_code = {str(subc['coInSub']): subc for subc in meta_subcontratas}
    cv_by_code = {str(dept['code']): dept for dept in cv_departments if dept['code'] != '1'}

    meta_subc_codes = set(meta_by_code.keys())
    cv_dept_codes = set(cv_by_code.keys())

    new_dept_codes = meta_subc_codes - cv_dept_codes
    deleted_codes = cv_dept_codes - meta_subc_codes

    if len(new_dept_codes) > 0:
        logging.info(f"Starting the creation of {len(new_dept_codes)} departments")
        for dept in [meta_by_code[code] for code in new_dept_codes]:
            # CV Security doesn't allow repeated department names, so the {Code - Name} nomenclature will be used
            new_dept = Department(code=str(dept['coInSub']),
                                  name=f"{dept['coInSub']}-{clean_string(dept['nombre'], 'alphanumeric')}")
            try:
                await cv_departments_api.add_or_edit_department(new_dept)
            except Exception as e:
                logger.error(f"Error creating department/subcontrata: {new_dept.model_dump_json()}", exc_info=e)
    else:
        logging.info("No new departments/subcontrata to create")

    if len(deleted_codes) > 0:
        # CVSecurity has a default department code '1'. This must be excluded
        logging.info(f"Starting the deletion of {len(deleted_codes)} departments")
        for code in deleted_codes:
            try:
                await cv_departments_api.delete_department(code)
            except Exception as e:
                logger.error(f"Error deleting department/subcontrata code: {code}", exc_info=e)
    else:
        logging.info("No departments/subcontrata to delete")

    return
