from prefect import flow, task, get_run_logger
from datetime import datetime
import subprocess
import os

@task(
    name="Kepatuhan Identifikasi Pasien",
    retries=3,
    retry_delay_seconds=60,
    persist_result=True
)
def execute_spark_job(jar, catalog, resource, start_date):
    logger = get_run_logger()
    script_path = "./run_spark.sh"
    cmd = ["bash", script_path, jar, catalog, resource, start_date]
    logger.info(f"Starting Spark job: {resource} | date: {start_date}")

    process = subprocess.Popen(
        cmd,
        cwd=os.getcwd(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    for line in iter(process.stdout.readline, ""):
        line = line.rstrip()
        if line:
            logger.info(line)

    process.stdout.close()
    process.wait()

    if process.returncode != 0:
        raise Exception(
            f"Spark job '{resource}' failed with exit code {process.returncode}"
        )

    logger.info(f"Spark job '{resource}' finished successfully")
    return True


@flow(name="Kepatuhan Identifikasi Pasien")
def kepatuhan_identifikasi_pasien_flow(start_date: str = datetime.now().strftime("%Y-%m-%d")):
    logger = get_run_logger()
    logger.info(f"Starting flow for date: {start_date}")

    f1 = execute_spark_job.submit(
        "etl-pipeline-assembly-1.0.0.jar",
        "fg_fact_mutu_identifikasi_pasien",
        "fg_fact_mutu_identifikasi_pasien",
        start_date
    )

    f1.result()
    logger.info("Flow completed successfully")


if __name__ == "__main__":
    kepatuhan_identifikasi_pasien_flow()
