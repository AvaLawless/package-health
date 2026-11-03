from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository
from prefect.schedules import Cron

from main import package_health

GIT_URL = "https://github.com/AvaLawless/package-health.git"
WORK_POOL = "uvasds-k8s-pool"

if __name__ == "__main__":
    # runs midnight Eastern on the 28th of every month
    package_health.from_source(
        source=GitRepository(
            url=GIT_URL,
             branch="main",
            credentials={"access_token": Secret.load("psz6ah-pat")},
        ),
        entrypoint="main.py:package_health",
    ).deploy(
        name="psz6ah-deployment",
        work_pool_name=WORK_POOL,
        schedule=Cron("0 0 28 * *", timezone="America/New_York"),
        job_variables={
            "cpu_request": "2",
            "cpu_limit": "2",
            "memory_request": "2Gi",
            "memory_limit": "4Gi",
        },
    )