from prefect import flow, task, get_run_logger
from datetime import datetime, timedelta
import subprocess


# =====================================================
# TASK EXECUTOR (UNCHANGED CORE)
# =====================================================
@task(
    name="Rekap Penggunaan Barang",
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
@flow(name="Rekap Penggunaan Barang")
def rekap_penggunaan_barang_flow(
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
            "fg_dim_pengemasan_barang",
            "fg_dim_pengemasan_barang",
            run_date
        )

        f2 = execute_spark_job.submit(
            jar,
            "fg_dim_nakes",
            "fg_dim_nakes",
            run_date
        )

        # =================================================
        # STAGE 2: FACT (wait dim)
        # =================================================
        f3 = execute_spark_job.submit(
            jar,
            "fg_fact_penjualan_barang",
            "fg_fact_penjualan_barang",
            run_date,
            wait_for=[f1, f2]
        )

        f4 = execute_spark_job.submit(
            jar,
            "fg_fact_retur_barang",
            "fg_fact_retur_barang",
            run_date,
            wait_for=[f1, f2]
        )

        f4b = execute_spark_job.submit(
            jar,
            "fg_fact_pergerakan_stok",
            "fg_fact_pergerakan_stok",
            run_date,
            wait_for=[f1]
        )

        # =================================================
        # STAGE 3: AGG (final)
        # =================================================
        f5 = execute_spark_job.submit(
            jar,
            "fg_agg_rekap_penggunaan_barang",
            "fg_agg_rekap_penggunaan_barang",
            run_date,
            wait_for=[f1, f2, f3, f4]
        )

        f5b = execute_spark_job.submit(
            jar,
            "fg_fact_pergerakan_stok_bulanan",
            "fg_fact_pergerakan_stok_bulanan",
            run_date,
            wait_for=[f4b]
        )

        f5.result()
        f5b.result()

    logger.info("ALL DATES COMPLETED SUCCESSFULLY")


# =====================================================
# ENTRYPOINT
# =====================================================
if __name__ == "__main__":
    rekap_penggunaan_barang_flow()