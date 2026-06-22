from prefect import flow, task, get_run_logger
from datetime import datetime
import subprocess
import os

@task(
    name="Kepatuhan Upaya Pencegahan Risiko Pasien Jatuh",
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


@flow(name="Kepatuhan Upaya Pencegahan Risiko Pasien Jatuh")
def kepatuhan_upaya_pencegahan_risiko_pasien_jatuh_flow(start_date: str = datetime.now().strftime("%Y-%m-%d")):
    logger = get_run_logger()
    logger.info(f"Starting flow for date: {start_date}")

    f1 = execute_spark_job.submit(
        "etl-pipeline-assembly-1.0.0.jar",
        "fg_fact_mutu_pasien_jatuh",
        "fg_fact_mutu_pasien_jatuh",
        start_date
    )

    f1.result()
    logger.info("Flow completed successfully")


if __name__ == "__main__":
    kepatuhan_upaya_pencegahan_risiko_pasien_jatuh_flow()