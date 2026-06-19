from prefect import flow, task, get_run_logger
from datetime import datetime
import subprocess

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
        cwd="/opt/prefect/projects",
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


@flow(name="Rekap Penggunaan Barang")
def rekap_penggunaan_barang_flow(start_date: str = datetime.now().strftime("%Y-%m-%d")):
    logger = get_run_logger()
    logger.info(f"Starting flow for date: {start_date}")

    # Stage 1: Dimension tables (paralel)
    f1 = execute_spark_job.submit(
        "etl-pipeline-assembly-1.0.0.jar",
        "fg_dim_pengemasan_barang",
        "fg_dim_pengemasan_barang",
        start_date
    )
    f2 = execute_spark_job.submit(
        "etl-pipeline-assembly-1.0.0.jar",
        "fg_dim_nakes",
        "fg_dim_nakes",
        start_date
    )

    # Stage 2: Fact tables (paralel, tunggu f1 & f2)
    f3 = execute_spark_job.submit(
        "etl-pipeline-assembly-1.0.0.jar",
        "fg_fact_penjualan_barang",
        "fg_fact_penjualan_barang",
        start_date,
        wait_for=[f1, f2]
    )
    f4 = execute_spark_job.submit(
        "etl-pipeline-assembly-1.0.0.jar",
        "fg_fact_retur_barang",
        "fg_fact_retur_barang",
        start_date,
        wait_for=[f1, f2]
    )

    # Stage 3: Rekap final (tunggu semua)
    f5 = execute_spark_job.submit(
        "etl-pipeline-assembly-1.0.0.jar",
        "fg_fact_rekap_penggunaan_barang",
        "fg_fact_rekap_penggunaan_barang",
        start_date,
        wait_for=[f1, f2, f3, f4]
    )

    f5.result()
    logger.info("Flow completed successfully")


if __name__ == "__main__":
    rekap_penggunaan_barang_flow.serve(
        name="rekap-penggunaan-barang",
        cron="0 1 * * *",  # setiap hari jam 01:00
    )