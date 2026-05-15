from prefect import flow, task
from datetime import datetime
import subprocess
import os

@task(name="Execute Spark Script")
def execute_spark_job(jar, catalog, resource, start_date):
    # Tentukan path ke script run_spark.sh Anda
    script_path = "/opt/prefect/projects/run_spark.sh"
    
    # Perintah: ./run_spark.sh <jar> <catalog> <resource> <start_date>
    cmd = [
        "bash", script_path, 
        jar, 
        catalog, 
        resource, 
        start_date
    ]
    
    print(f"Running job for resource: {resource} on date: {start_date}")
    
    # Jalankan subprocess
    process = subprocess.run(
        cmd, 
        cwd="/opt/prefect/projects", # Pastikan run dari root project
        capture_output=True, 
        text=True
    )
    
    if process.returncode != 0:
        print(f"Error Output: {process.stderr}")
        raise Exception(f"Spark job {resource} failed with exit code {process.returncode}")
    
    print(f"Output: {process.stdout}")
    return True

@flow(name="Rekap Penggunaan Barang")
def rekap_penggunaan_barang_flow():
    # Ambil tanggal hari ini format YYYY-MM-DD
    today = datetime.now().strftime("%Y-%m-%d")
    
    # 1. Jalankan Dependensi Fact (Harian)
    # fg_fact_penjualan_barang
    f1 = execute_spark_job(
        "etl_pipeline-assembly-1.0.0.jar", 
        "fg_fact_penjualan_barang", 
        "fg_fact_penjualan_barang", 
        today
    )
    
    # fg_fact_retur_barang
    f2 = execute_spark_job(
        "etl_pipeline-assembly-1.0.0.jar", 
        "fg_fact_retur_barang", 
        "fg_fact_retur_barang", 
        today
    )
    
    # 2. Jalankan Rekap Utama (Hanya jika Fact Penjualan & Retur Sukses)
    execute_spark_job(
        "etl_pipeline-assembly-1.0.0.jar", 
        "rekap_penggunaan_barang", 
        "rekap_penggunaan_barang", 
        today,
        wait_for=[f1, f2] # Menjamin urutan
    )

if __name__ == "__main__":
    rekap_penggunaan_barang_flow()