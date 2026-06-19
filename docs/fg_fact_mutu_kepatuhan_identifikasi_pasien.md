# S2TM – fg_fact_mutu_kepatuhan_identifikasi_pasien

| Target Table          | Target Column              | Target Data Type | Source Table          | Source Column                                 | Transformation / Logic                                       | Join Type | Join Key | Notes                              |
| --------------------- | -------------------------- | ---------------: | --------------------- | --------------------------------------------- | ------------------------------------------------------------ | --------- | -------- | ---------------------------------- |
| document_history_list | id                         |           BIGINT | document_history_list | document_history_id                           | `cast(document_history_id as bigint)`                        | —         | —        | Primary key dari dokumen observasi |
| document_history_list | no_rm                      |      VARCHAR(50) | emr_data              | `$.pasien.no_rm`                              | `get_json_object(emr_data,'$.pasien.no_rm')`                 | —         | —        | Nomor rekam medis pasien           |
| document_history_list | pasien                     |     VARCHAR(255) | emr_data              | `$.pasien.nama`                               | `get_json_object(emr_data,'$.pasien.nama')`                  | —         | —        | Nama pasien                        |
| document_history_list | verifikasi                 |     VARCHAR(100) | data_list             | profesi                                       | `explode(data_list).profesi`                                 | —         | —        | Profesi pemberi pelayanan          |
| document_history_list | nama_verifikator           |     VARCHAR(255) | data_list             | paraf                                         | `explode(data_list).paraf`                                   | —         | —        | Nama petugas/verifikator           |
| document_history_list | identifikasi_nama          |      VARCHAR(10) | data_list             | identifikasi_nama                             | `WHEN lower(trim(value))='true' THEN 'Ya' ELSE 'Tidak'`      | —         | —        | Status identifikasi nama pasien    |
| document_history_list | identifikasi_tanggal_lahir |      VARCHAR(10) | data_list             | identifikasi_tanggal_lahir                    | `WHEN lower(trim(value))='true' THEN 'Ya' ELSE 'Tidak'`      | —         | —        | Status identifikasi tanggal lahir  |
| document_history_list | status                     |      VARCHAR(20) | Derived               | identifikasi_nama, identifikasi_tanggal_lahir | `WHEN keduanya='true' THEN 'Memenuhi' ELSE 'Tidak Memenuhi'` | —         | —        | Penentuan kepatuhan indikator      |
| Generated             | tahun                      |         SMALLINT | created_time          | created_time                                  | `year(to_date(created_time))`                                | —         | —        | Tahun observasi                    |
| Generated             | triwulan                   |       VARCHAR(5) | created_time          | created_time                                  | `concat('Q', quarter(to_date(created_time)))`                | —         | —        | Triwulan observasi                 |
| Generated             | nama_bulan                 |      VARCHAR(20) | created_time          | created_time                                  | Mapping bulan menjadi `01-Januari` s.d. `12-Desember`        | —         | —        | Label bulan                        |
| Generated             | partition_date             |             DATE | created_time          | created_time                                  | `to_date(created_time)`                                      | —         | —        | Digunakan sebagai partisi data     |

## Business Rules

1. Sumber data berasal dari `document_history_list` dengan filter:

   * `document_code = 'RM102'`

2. Data observasi diekstrak dari JSON:

   * `pasien.no_rm`
   * `pasien.nama`
   * `data_list`

3. Array `data_list` di-`explode` sehingga setiap petugas observasi menjadi satu baris data.

4. Identifikasi pasien dinyatakan **Memenuhi** apabila:

   * `identifikasi_nama = true`, dan
   * `identifikasi_tanggal_lahir = true`

5. Selain kondisi tersebut, status ditetapkan sebagai:

   * `Tidak Memenuhi`

6. Record tanpa `nama_verifikator` tidak dimuat ke fact table:

   * `length(trim(nama_verifikator)) > 0`

## Definisi Indikator Mutu

* **Nama Indikator:** Kepatuhan Identifikasi Pasien
* **Jenis Indikator:** Proses
* **Target:** 100%
* **Frekuensi Pengumpulan:** Bulanan

### Numerator

Jumlah pemberi pelayanan yang melakukan identifikasi pasien dengan benar menggunakan dua identifikator (nama dan tanggal lahir) pada periode observasi.

### Denominator

Jumlah seluruh pemberi pelayanan yang diobservasi pada periode observasi.

### Kriteria Sukses

Pemberi pelayanan melakukan identifikasi pasien menggunakan dua identifikator:

1. Nama pasien
2. Tanggal lahir pasien

### Formula Capaian

`(Jumlah status = 'Memenuhi' / Jumlah observasi) × 100%`
