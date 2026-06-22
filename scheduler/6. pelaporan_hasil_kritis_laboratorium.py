from prefect import flow, task, get_run_logger
from datetime import datetime
import subprocess
import os

@task(
    name="Pelaporan Hasil Kritis Laboratorium",
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


@flow(name="Pelaporan Hasil Kritis Laboratorium")
def pelaporan_hasil_kritis_laboratorium_flow(start_date: str = datetime.now().strftime("%Y-%m-%d")):
    logger = get_run_logger()
    logger.info(f"Starting flow for date: {start_date}")

    f1 = execute_spark_job.submit(
        "etl-pipeline-assembly-1.0.0.jar",
        "fg_staging_document_history_list",
        "fg_staging_document_history_list",
        start_date
    )

    f2 = execute_spark_job.submit(
        "etl-pipeline-assembly-1.0.0.jar",
        "fg_fact_mutu_hasil_lab_kritis",
        "fg_fact_mutu_hasil_lab_kritis",
        start_date,
        wait_for=[f1]
    )

    f2.result()
    logger.info("Flow completed successfully")


if __name__ == "__main__":
    pelaporan_hasil_kritis_laboratorium_flow()