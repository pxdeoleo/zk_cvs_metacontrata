from typing import Optional, Dict, Any
from urllib.parse import urljoin
import httpx


class MetaContrataClient:
    def __init__(self, username: str, password: str, base_url: str) -> None:
        self.username: str = username
        self.password: str = password
        self.base_url: str = base_url
        self.passkey: Optional[str] = None
        self.client: Optional[httpx.AsyncClient] = None

    @property
    def login_endpoint(self) -> str:
        return urljoin(self.base_url, "login/passkey")

    @property
    def employee_list_endpoint(self) -> str:
        return urljoin(self.base_url, "empleados/listado")

    @property
    def subcontrata_list_endpoint(self) -> str:
        return urljoin(self.base_url, "empresas/listado")

    async def __aenter__(self) -> "MetaContrataClient":
        self.client = httpx.AsyncClient()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        if self.client:
            await self.client.aclose()

    async def authenticate(self) -> None:
        if not self.client:
            raise RuntimeError("Client not initialized. Use 'async with MetaContrataClient(...)'")

        response: httpx.Response = await self.client.post(
            self.login_endpoint,
            json={
                "usuario": self.username,
                "password": self.password
            }
        )

        response.raise_for_status()
        data: Dict[str, Any] = response.json()

        if data["estado"] != 1:
            raise Exception(f"Authentication failed: {data['mensaje']}")

        self.passkey = data["resultado"]["passkey"]

    async def get_employee_list(
            self,
            solo_activos: int = 1,
            co_in_contrat: Optional[str] = None,
            co_in_ct: Optional[str] = None,
            co_in_sub: Optional[str] = None,
            cif_empresa: Optional[str] = None,
            co_in_empl: Optional[str] = None,
            nif_empleado: Optional[str] = None
    ) -> Any:
        if not self.passkey:
            raise Exception("Authentication required. Please call authenticate() first.")
        if not self.client:
            raise RuntimeError("Client not initialized.")

        headers: Dict[str, str] = {"passkey": self.passkey}

        payload: Dict[str, Any] = {"SoloEmpleadosActivos": solo_activos}
        if co_in_contrat:
            payload["CoInContrat"] = co_in_contrat
        if co_in_ct:
            payload["CoInCT"] = co_in_ct
        if co_in_sub:
            payload["CoInSub"] = co_in_sub
        if cif_empresa:
            payload["CifEmpresa"] = cif_empresa
        if co_in_empl:
            payload["CoInEmpl"] = co_in_empl
        if nif_empleado:
            payload["NifEmpleado"] = nif_empleado

        response: httpx.Response = await self.client.post(
            self.employee_list_endpoint,
            json=payload,
            headers=headers
        )

        response.raise_for_status()
        data: Dict[str, Any] = response.json()

        if data["estado"] != 1:
            raise Exception(f"Error fetching employee list: {data['mensaje']}")

        return data["resultado"]

    async def get_subcontrata_list(
            self,
            solo_activos: Optional[bool] = None,
            co_in_contrat: Optional[str] = None,
            co_in_ct: Optional[str] = None,
            co_in_sub: Optional[str] = None,
            cif_empresa: Optional[str] = None
    ) -> Any:
        if not self.passkey:
            raise Exception("Authentication required. Please call authenticate() first.")
        if not self.client:
            raise RuntimeError("Client not initialized.")

        headers: Dict[str, str] = {"passkey": self.passkey}

        payload: Dict[str, Any] = {}
        if co_in_contrat:
            payload["CoInContrat"] = co_in_contrat
        if co_in_ct:
            payload["CoInCT"] = co_in_ct
        if co_in_sub:
            payload["CoInSub"] = co_in_sub
        if solo_activos is not None:
            payload["SoloActivas"] = 1 if solo_activos else 0
        if cif_empresa:
            payload["CifEmpresa"] = cif_empresa

        response: httpx.Response = await self.client.post(
            self.subcontrata_list_endpoint,
            json=payload,
            headers=headers
        )

        response.raise_for_status()
        data: Dict[str, Any] = response.json()

        if data["estado"] != 1:
            raise Exception(f"Error fetching subcontrata list: {data['mensaje']}")

        return data["resultado"]
