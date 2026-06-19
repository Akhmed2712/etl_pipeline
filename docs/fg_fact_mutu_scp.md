# S2TM – fg_fact_mutu_scp

| Target Table | Target Column | Target Data Type | Source Table | Source Column | Transformation / Logic | Join Type | Join Key | Notes |
|-------------|--------------|-----------------|-------------|--------------|------------------------|-----------|----------|-------|
| fg_fact_mutu_scp | id | BIGINT | document_history_list | document_history_id | `cast(document_history_id as bigint)` | Inner | document_history_id | Primary key fact table |
| fg_fact_mutu_scp | no_rm | VARCHAR(50) | emr_data | `$.pasien.no_rm` | `get_json_object(emr_data,'$.pasien.no_rm')` | Inner | document_history_id | Nomor rekam medis pasien |
| fg_fact_mutu_scp | nama_pasien | VARCHAR(255) | emr_data | `$.pasien.nama_pasien` | `get_json_object(emr_data,'$.pasien.nama_pasien')` | Inner | document_history_id | Nama pasien |
| fg_fact_mutu_scp | dokter_bedah | VARCHAR(255) | emr_data | `$.staf.dokter_operator` | `get_json_object(emr_data,'$.staf.dokter_operator')` | Inner | document_history_id | Dokter operator tindakan bedah |
| fg_fact_mutu_scp | icd_x | VARCHAR(20) | emr_data | `$.asesmen.diagnosa_pra_operasi[0].icd_x` | `get_json_object(emr_data,'$.asesmen.diagnosa_pra_operasi[0].icd_x')` | Inner | document_history_id | Kode ICD-X diagnosa pra operasi |
| fg_fact_mutu_scp | diagnosa | TEXT | emr_data | `$.asesmen.diagnosa_pra_operasi[0].nama_sebab_sakit` | `get_json_object(emr_data,'$.asesmen.diagnosa_pra_operasi[0].nama_sebab_sakit')` | Inner | document_history_id | Diagnosa pra operasi |
| fg_fact_mutu_scp | tanggal_operasi | DATE | emr_data | `$.waktu.tgl_bedah` | `get_json_object(emr_data,'$.waktu.tgl_bedah')` | Inner | document_history_id | Tanggal operasi |
| fg_fact_mutu_scp | jam_asesmen | VARCHAR(10) | emr_data | `$.asesmen.jam` | `get_json_object(emr_data,'$.asesmen.jam')` | Inner | document_history_id | Jam asesmen pre operasi |
| fg_fact_mutu_scp | jam_operasi | VARCHAR(10) | emr_data | `$.waktu.mulai_operasi` | `get_json_object(emr_data,'$.waktu.mulai_operasi')` | Inner | document_history_id | Jam mulai operasi |
| fg_fact_mutu_scp | selisih_menit | BIGINT | Derived | jam_asesmen, jam_operasi | `(unix_timestamp(ts_operasi)-unix_timestamp(ts_asesmen))/60` | — | — | Selisih waktu antara asesmen dan operasi |
| fg_fact_mutu_scp | status | VARCHAR(50) | Derived | selisih_menit | `WHEN selisih_menit <= 30 THEN 'Memenuhi' ELSE 'Tidak Memenuhi'` | — | — | Status kepatuhan indikator |
| Generated | tahun | INT | tanggal_operasi | tanggal_operasi | `year(to_date(tanggal_operasi))` | — | — | Tahun operasi |
| Generated | triwulan | VARCHAR(5) | tanggal_operasi | tanggal_operasi | `concat('Q', quarter(to_date(tanggal_operasi)))` | — | — | Triwulan operasi |
| Generated | nama_bulan | VARCHAR(20) | tanggal_operasi | tanggal_operasi | Mapping bulan menjadi `01-Januari` s.d. `12-Desember` | — | — | Label bulan |
| Generated | partition_date | DATE | tanggal_operasi | tanggal_operasi | `to_date(tanggal_operasi)` | — | — | Digunakan sebagai partisi data |

## Business Rules

1. Sumber data berasal dari `document_history_list` dengan filter:

   * `title IN ('Laporan Operasi','Serah Terima Pasien Pre Operasi')`
   * `created_time < awal bulan berjalan`

2. Dataset **Laporan Operasi** diambil dari dokumen:

   * `document_code = 'RS52_RM4.10'`
   * Hanya operasi dengan status:
     * `laporan.kualifikasi_2.emergency = true`

3. Dataset **Pre Operasi** diambil dari dokumen:

   * `document_code = 'RM55_K'`

4. Kedua dataset digabung menggunakan:

   * `INNER JOIN document_history_id`

5. Timestamp asesmen dibentuk dari:

   * `tanggal_operasi + jam_asesmen`

6. Timestamp operasi dibentuk dari:

   * `tanggal_operasi + jam_operasi`

7. Selisih waktu dihitung dalam satuan menit:

   * `(waktu_operasi - waktu_asesmen)`

8. Status indikator ditentukan sebagai:

   * `Memenuhi`, apabila `selisih_menit <= 30`
   * `Tidak Memenuhi`, apabila `selisih_menit > 30`

9. Field periode (`tahun`, `triwulan`, `nama_bulan`, `partition_date`) dibentuk dari `tanggal_operasi`.

## Definisi Indikator Mutu

* **Nama Indikator:** Kepatuhan Surgical Safety Checklist (SCP)
* **Jenis Indikator:** Proses
* **Target:** 100%
* **Frekuensi Pengumpulan:** Bulanan

### Numerator

Jumlah pasien operasi emergensi yang dilakukan asesmen pre operasi dengan selisih waktu maksimal 30 menit sebelum tindakan operasi.

### Denominator

Jumlah seluruh pasien operasi emergensi yang memiliki data asesmen pre operasi dan laporan operasi pada periode pengukuran.

### Kriteria Sukses

Pasien operasi emergensi memenuhi ketentuan:

1. Terdapat dokumen asesmen pre operasi.
2. Terdapat dokumen laporan operasi.
3. Selisih waktu antara asesmen dan mulai operasi tidak lebih dari 30 menit.

### Formula Capaian

`(Jumlah status = 'Memenuhi' / Jumlah seluruh operasi emergensi) × 100%`