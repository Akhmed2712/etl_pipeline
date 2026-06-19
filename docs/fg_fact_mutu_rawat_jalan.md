# S2TM – fg_fact_mutu_rawat_jalan

| Target Table    |  Target Column | Target Data Type | Source Table                       | Source Column | Transformation / Logic                                           | Join Type | Join Key                              | Notes                            |
| --------------- | -------------: | ---------------: | ---------------------------------- | ------------- | ---------------------------------------------------------------- | --------- | ------------------------------------- | -------------------------------- |
| dc_pendaftaran  |             id |           BIGINT | dc_pendaftaran                     | id            | `cast(id as bigint)`                                             | —         | —                                     | Primary key pendaftaran          |
| dc_pendaftaran  |    no_register |      VARCHAR(30) | dc_pendaftaran                     | no_register   | Direct mapping                                                   | —         | —                                     | Nomor registrasi pasien          |
| dc_pendaftaran  |          no_rm |      VARCHAR(10) | dc_pendaftaran                     | id_pasien     | `lpad(substring(regexp_replace(id_pasien,'^0+',''),-6,6),6,'0')` | —         | —                                     | Normalisasi nomor rekam medis    |
| dc_pegawai      |    nama_dokter |     VARCHAR(255) | dc_pegawai                         | nama          | Join melalui nakes dan pegawai                                   | LEFT      | `id_dokter → id_pegawai → pegawai_id` | Nama dokter pemeriksa            |
| dc_spesialisasi |   spesialisasi |     VARCHAR(255) | dc_spesialisasi                    | nama          | Direct mapping hasil join                                        | LEFT      | `id_unit_layanan = id_spesialisasi`   | Nama spesialisasi/poli           |
| dc_taskid_bpjs  |   waktu_task_3 |        TIMESTAMP | dc_taskid_bpjs                     | waktu_rs      | Filter `task_id = 3`                                             | INNER     | `no_register`                         | Waktu selesai pendaftaran        |
| dc_taskid_bpjs  |   waktu_task_4 |        TIMESTAMP | dc_taskid_bpjs                     | waktu_rs      | Filter `task_id = 4`                                             | INNER     | `no_register`                         | Waktu mulai pelayanan dokter     |
| Derived         |  selisih_menit |           BIGINT | waktu_task_3, waktu_task_4         | waktu_rs      | `(unix_timestamp(task4)-unix_timestamp(task3))/60`               | —         | —                                     | Waktu tunggu pelayanan dokter    |
| Derived         |         status |     VARCHAR(100) | spesialisasi, jenis, selisih_menit | Derived       | Penentuan status mutu                                            | —         | —                                     | Memenuhi/Tidak Memenuhi/Eksklusi |
| Generated       |          tahun |              INT | waktu_task_3                       | waktu_task_3  | `year(to_date(waktu_task_3))`                                    | —         | —                                     | Tahun observasi                  |
| Generated       |       triwulan |       VARCHAR(5) | waktu_task_3                       | waktu_task_3  | `concat('Q', quarter(to_date(waktu_task_3)))`                    | —         | —                                     | Triwulan observasi               |
| Generated       |     nama_bulan |      VARCHAR(20) | waktu_task_3                       | waktu_task_3  | Mapping `01-Januari` s.d. `12-Desember`                          | —         | —                                     | Label bulan                      |
| Generated       | partition_date |             DATE | waktu_task_3                       | waktu_task_3  | `to_date(waktu_task_3)`                                          | —         | —                                     | Partisi data                     |

## Business Rules

1. Data pendaftaran diambil dari `dc_pendaftaran` dengan filter:

   * `rawat = 'jalan'`
   * `status NOT IN ('Batal','Booking')`

2. Data layanan diambil dari `dc_layanan_pendaftaran` dengan filter:

   * `status NOT IN ('Batal','Belum')`

3. Untuk setiap pendaftaran dipilih **layanan pertama** menggunakan:

   ```sql
   row_number() over (
       partition by id_pendaftaran
       order by waktu asc
   )
   ```

4. Data BPJS digunakan untuk mendapatkan:

   * **Task ID 3** = selesai administrasi/pendaftaran
   * **Task ID 4** = mulai pelayanan dokter

5. Waktu tunggu dihitung menggunakan:

   ```sql
   selisih_menit =
   (waktu_task_4 - waktu_task_3)/60
   ```

6. Status **Eksklusi** apabila:

   * Spesialisasi = `gigi`
   * Jenis pelayanan = `MCU`
   * Nama spesialisasi mengandung `MCU`

7. Status **Memenuhi** apabila:

   ```sql
   selisih_menit <= 60
   ```

8. Status **Tidak Memenuhi** apabila:

   ```sql
   selisih_menit > 60
   ```

## Definisi Indikator Mutu

* **Nama Indikator:** Waktu Tunggu Rawat Jalan
* **Jenis Indikator:** Proses
* **Target:** > 80%
* **Frekuensi Pengumpulan:** Bulanan

### Numerator

Jumlah pasien rawat jalan dengan waktu tunggu ≤ 60 menit.

### Denominator

Jumlah pasien rawat jalan yang diobservasi (tidak termasuk eksklusi).

### Kriteria Sukses

Pasien mendapatkan pelayanan dokter/spesialis dalam waktu ≤ 60 menit sejak kontak dengan petugas pendaftaran.

### Kriteria Eksklusi

* Pasien medical check up (MCU).
* Pasien poli gigi.
* Pasien daftar online yang datang > 60 menit dari waktu yang ditentukan.
* Pasien kunjungan mandiri yang datang > 60 menit dari waktu yang ditentukan.
* Pasien yang memiliki tindakan sebelumnya.

> **Catatan ETL:** Pada implementasi saat ini, eksklusi yang telah diterapkan pada ETL adalah pasien MCU dan poli gigi.

### Formula Capaian

```text
(Jumlah status = 'Memenuhi')
/
(Jumlah status != 'Eksklusi')
× 100%
```
