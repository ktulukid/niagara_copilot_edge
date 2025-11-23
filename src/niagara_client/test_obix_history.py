from __future__ import annotations

from datetime import datetime, timedelta

from .history_http_obix import NiagaraObixHistoryClient


def main() -> None:
    # 1) Fill these in
    base_url = "http://172.20.40.22/obix"
    username = "AMS"
    password = "AmsDDC6810"

    # 2) This path must point to the *historyQuery* for your VAV space temp
    # Example shape (you will adjust to match your station):
    #   /histories/AmsShop/Vav1-11-SpaceTemperature/-/historyQuery
    history_query_path = "/histories/AmsShop/Vav1-11-SpaceTemperature/-/historyQuery"

    client = NiagaraObixHistoryClient(
        base_url=base_url,
        username=username,
        password=password,
    )

    end = datetime.now().astimezone()
    start = end - timedelta(hours=24)

    df = client.fetch_history(history_query_path, start=start, end=end)

    print(df.head())
    print()
    print(f"samples: {len(df)}")
    if not df.empty:
        print(f"range: {df['timestamp'].min()}  ->  {df['timestamp'].max()}")


if __name__ == "__main__":
    main()
