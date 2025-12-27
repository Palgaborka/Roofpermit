from connectors.base import Jurisdiction, PermitConnector
from connectors.energov import EnerGovConnector

def get_connector(j: Jurisdiction) -> PermitConnector:
    system = (j.system or "").lower().strip()
    if system == "energov":
        return EnerGovConnector(j)
    raise ValueError(f"Unsupported system: {j.system}")
