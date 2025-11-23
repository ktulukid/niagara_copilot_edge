from __future__ import annotations

from pprint import pprint

from .analytics_api import AnalyticsApiClient


def main() -> None:
    client = AnalyticsApiClient(
        base_url="http://172.20.40.22/na",  # servlet name 'na'
        username="AMS",
        password="AmsDDC6810",      # temporary hard-code for test
        timeout=10,
        verify_ssl=False,                   # HTTP on JACE, so SSL verify off is fine
    )

    # Call GetNode on the root slot
    node = client.get_node("slot:/Drivers/BacnetNetwork/Vav1$2d11/points/Reporting/SpaceTemperature")

    print("=== Raw Pydantic object ===")
    print(node)

    print("\n=== As dict ===")
    pprint(node.model_dump())


if __name__ == "__main__":
    main()
