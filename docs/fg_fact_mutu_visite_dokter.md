# S2TM – fg_fact_mutu_visite_dokter

| Target Table          | Target Column  | Target Data Type | Source Table                         | Source Column             | Transformation / Logic                         | Join Type | Join Key | Notes                            |
| --------------------- | -------------- | ---------------: | ------------------------------------ | ------------------------- | ---------------------------------------------- | --------- | -------- | -------------------------------- |
| document_history_list | id             |           BIGINT | document_history_list                | document_history_list_id  | `cast(document_history_list_id as bigint)`     | —         | —        | Primary key dokumen CPPT         |
| document_history_list | no_rm          |             TEXT | emr_data                             | `$.pasien.no_rm`          | `get_json_object(emr_data,'$.pasien.no_rm')`   | —         | —        | Nomor rekam medis pasien         |
| document_history_list | pasien         |     VARCHAR(255) | emr_data                             | `$.pasien.nama`           | `get_json_object(emr_data,'$.pasien.nama')`    | —         | —        | Nama pasien                      |
| document_history_list | klinik         |     VARCHAR(255) | data_list                            | klinik                    | `explode(data_list).klinik`                    | —         | —        | Unit/klinik pelayanan            |
| document_history_list | dokter         |     VARCHAR(255) | data_list                            | verifikasi_dpjp.nama_dpjp | `explode(data_list).verifikasi_dpjp.nama_dpjp` | —         | —        | Nama DPJP                        |
| document_history_list | tanggal_masuk  |             DATE | document_history_list                | created_time              | `to_date(created_time)`                        | —         | —        | Tanggal pasien masuk/rawat       |
| document_history_list | tanggal_visite |      VARCHAR(20) | data_list                            | tanggal_cppt              | `explode(data_list).tanggal_cppt`              | —         | —        | Tanggal visite dokter            |
| document_history_list | jam            |      VARCHAR(10) | data_list                            | jam_cppt                  | `explode(data_list).jam_cppt`                  | —         | —        | Jam visite dokter                |
| document_history_list | pemeriksaan    |             TEXT | data_list                            | pilihan                   | `explode(data_list).pilihan`                   | —         | —        | Jenis pemeriksaan/CPPT           |
| Derived               | status         |      VARCHAR(50) | created_time, tanggal_cppt, jam_cppt | Derived                   | Penentuan status mutu                          | —         | —        | Memenuhi/Tidak Memenuhi/Eksklusi |
| Generated             | tahun          |              INT | tanggal_cppt                         | tanggal_cppt              | `year(to_date(tanggal_cppt))`                  | —         | —        | Tahun observasi                  |
| Generated             | triwulan       |       VARCHAR(5) | tanggal_cppt                         | tanggal_cppt              | `concat('Q', quarter(to_date(tanggal_cppt)))`  | —         | —        | Triwulan observasi               |
| Generated             | nama_bulan     |      VARCHAR(20) | tanggal_cppt                         | tanggal_cppt              | Mapping `01-Januari` s.d. `12-Desember`        | —         | —        | Label bulan                      |
| Generated             | partition_date |             DATE | tanggal_cppt                         | tanggal_cppt              | `to_date(tanggal_cppt)`                        | —         | —        | Partisi data                     |

## Business Rules

1. Sumber data berasal dari `document_history_list` dengan filter:

   * `document_code = 'CPPT1'`
   * `label = 'Rawat Inap'`

2. Data berikut diekstrak dari JSON `emr_data`:

   * `pasien.no_rm`
   * `pasien.nama`
   * `data_list`

3. Array `data_list` di-`explode` sehingga setiap entri CPPT menjadi satu record observasi.

4. Hanya data dengan profesi **dokter** yang diproses:

   ```sql
   lower(item.profesi) = 'dokter'
   ```

5. Status **Eksklusi** apabila:

   ```sql
   to_date(tanggal_cppt) = to_date(created_time)
   ```

   Artinya pasien baru masuk pada hari yang sama dengan visite.

6. Status **Memenuhi** apabila jam visite berada pada rentang:

   ```sql
   06:00 – 14:00
   ```

7. Status **Tidak Memenuhi** apabila visite dilakukan di luar rentang waktu tersebut.

## Definisi Indikator Mutu

* **Nama Indikator:** Kepatuhan Visite Dokter DPJP
* **Jenis Indikator:** Proses
* **Target:** ≥ 80%
* **Frekuensi Pengumpulan:** Bulanan

### Numerator

Jumlah pasien rawat inap yang mendapatkan visite dokter DPJP sesuai waktu yang ditetapkan.

### Denominator

Jumlah pasien rawat inap yang diobservasi untuk visite dokter DPJP (tidak termasuk eksklusi).

### Kriteria Sukses

Dokter DPJP melakukan visite kepada pasien rawat inap pada waktu yang telah ditetapkan rumah sakit, yaitu pukul **06.00–14.00 WIB**.

| ✅ Memenuhi                                    | ❌ Tidak Memenuhi                               |
| --------------------------------------------- | ---------------------------------------------- |
| Visite dokter dilakukan pukul 06.00–14.00 WIB | Visite dilakukan di luar pukul 06.00–14.00 WIB |
| Terdokumentasi pada CPPT                      | Tidak terdokumentasi atau tidak sesuai waktu   |

### Kriteria Eksklusi

* Pasien baru masuk rawat inap pada hari yang sama dengan tanggal visite dokter.

### Formula Capaian

```text
(Jumlah status = 'Memenuhi')
/
(Jumlah status != 'Eksklusi')
× 100%
```
