from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "RoofSpy"
    database_url: str = "sqlite:///./roofspy.db"

    # West Palm Beach EnerGov "Self Service" search base
    energov_wpb_search_url: str = (
        "https://westpalmbeachfl-energovpub.tylerhost.net/apps/selfservice/"
        "WestPalmBeachFLProd#/search?m=2&ps=10&pn=1&em=true"
    )

    # how many days back to scan on each run
    ingest_days_back: int = 14

    # scheduler
    ingest_cron_hour_1: int = 7
    ingest_cron_hour_2: int = 13

settings = Settings()
