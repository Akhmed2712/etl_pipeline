from prefect import flow, task, get_run_logger
from datetime import datetime, timedelta
import subprocess


# =====================================================
# TASK EXECUTOR (UNCHANGED CORE)
# =====================================================
@task(
    name="Export Tarif",
    retries=3,
    retry_delay_seconds=60,
    persist_result=True
)
def execute_spark_job(jar, catalog, resource, start_date):
    logger = get_run_logger()

    script_path = "./run_spark.sh"

    cmd = [
        "bash",
        script_path,
        jar,
        catalog,
        resource,
        start_date
    ]

    logger.info(f"Running: {resource} | date={start_date}")

    process = subprocess.Popen(
        cmd,
        cwd="/opt/prefect",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    for line in iter(process.stdout.readline, ""):
        if line:
            logger.info(line.rstrip())

    process.stdout.close()
    process.wait()

    if process.returncode != 0:
        raise Exception(
            f"Spark job '{resource}' failed (code={process.returncode})"
        )

    return True


# =====================================================
# HELPER: DATE RANGE GENERATOR
# =====================================================
def generate_dates(start_date: str, end_date: str | None):
    start = datetime.strptime(start_date, "%Y-%m-%d")

    if not end_date:
        yield start_date
        return

    end = datetime.strptime(end_date, "%Y-%m-%d")

    current = start
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


# =====================================================
# MAIN FLOW (RANGE-AWARE)
# =====================================================
@flow(name="Export Tarif")
def export_tarif_flow(
    start_date: str = datetime.now().strftime("%Y-%m-%d"),
    end_date: str | None = None
):

    logger = get_run_logger()
    logger.info(f"Start flow: {start_date} -> {end_date}")

    jar = "etl-pipeline-assembly-1.0.0.jar"

    for run_date in generate_dates(start_date, end_date):

        logger.info(f"==============================")
        logger.info(f"RUNNING DATE: {run_date}")
        logger.info(f"==============================")

        # =================================================
        # STAGE 1: DIMENSIONS (parallel per day)
        # =================================================
        f1 = execute_spark_job.submit(
            jar,
            "fg_staging_komponen_tarif_detail",
            "fg_staging_komponen_tarif_detail",
            run_date
        )

        f2 = execute_spark_job.submit(
            jar,
            "fg_mart_export_tarif",
            "fg_mart_export_tarif",
            run_date,
            wait_for=[f1]
        )

        f2.result()

    logger.info("ALL DATES COMPLETED SUCCESSFULLY")


# =====================================================
# ENTRYPOINT
# =====================================================
if __name__ == "__main__":
    export_tarif_flow()