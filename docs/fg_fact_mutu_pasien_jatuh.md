# S2TM – fg_fact_mutu_pasien_jatuh

| Target Table          | Target Column   | Target Data Type | Source Table          | Source Column       | Transformation / Logic                       | Join Type | Join Key            | Notes                        |
| --------------------- | --------------- | ---------------: | --------------------- | ------------------- | -------------------------------------------- | --------- | ------------------- | ---------------------------- |
| document_history_list | id              |           BIGINT | document_history_list | document_history_id | `cast(document_history_id as bigint)`        | —         | —                   | Primary key dokumen asesmen  |
| document_history_list | no_rm           |      VARCHAR(50) | emr_data              | `$.pasien.no_rm`    | `get_json_object(emr_data,'$.pasien.no_rm')` | —         | —                   | Nomor rekam medis pasien     |
| document_history_list | pasien          |     VARCHAR(255) | emr_data              | `$.pasien.nama`     | `get_json_object(emr_data,'$.pasien.nama')`  | —         | —                   | Nama pasien                  |
| document_history_list | tanggal         |      VARCHAR(50) | RM21A/RM21B/RM21C     | tanggal/tgl         | Ekstraksi tanggal asesmen                    | —         | —                   | Tanggal asesmen risiko jatuh |
| document_history_list | bangsal         |      VARCHAR(50) | emr_data              | `$.pasien.bangsal`  | Ekstraksi JSON                               | —         | —                   | Ruang/bangsal pasien         |
| document_history_list | petugas         |     VARCHAR(255) | RM21A/RM21B/RM21C     | nama/paraf_petugas  | Ekstraksi petugas asesmen                    | —         | —                   | Nama petugas asesmen         |
| Derived               | total_skor      |              INT | RM21A/RM21B/RM21C     | Derived             | Perhitungan total skor risiko jatuh          | —         | —                   | Skor asesmen risiko jatuh    |
| Derived               | kategori_risiko |      VARCHAR(50) | total_skor/resiko     | Derived             | Penentuan kategori risiko                    | —         | —                   | Risiko Rendah/Sedang/Tinggi  |
| Window                | total_asesmen   |              INT | Derived               | Derived             | `max(rn) over(partition by no_rm)`           | —         | —                   | Jumlah asesmen pasien        |
| Derived               | asesmen_awal    |      VARCHAR(20) | total_asesmen         | Derived             | `>=1 → Dilakukan`                            | —         | —                   | Status asesmen awal          |
| Derived               | asesmen_ulang   |      VARCHAR(20) | total_asesmen         | Derived             | `>1 → Dilakukan`                             | —         | —                   | Status asesmen ulang         |
| document_history_list | intervensi      |      VARCHAR(20) | RM101                 | document_history_id | Ada dokumen RM101                            | LEFT      | document_history_id | Intervensi pencegahan jatuh  |
| Derived               | status          |      VARCHAR(30) | intervensi            | Derived             | Ada intervensi → Memenuhi                    | —         | —                   | Status indikator mutu        |
| Generated             | tahun           |              INT | partition_date        | partition_date      | `year(partition_date)`                       | —         | —                   | Tahun observasi              |
| Generated             | triwulan        |       VARCHAR(5) | partition_date        | partition_date      | `concat('Q', quarter(partition_date))`       | —         | —                   | Triwulan observasi           |
| Generated             | nama_bulan      |      VARCHAR(20) | partition_date        | partition_date      | Mapping bulan Indonesia                      | —         | —                   | Nama bulan                   |
| Generated             | partition_date  |             DATE | tanggal               | tanggal             | `to_date(tanggal)`                           | —         | —                   | Partisi data                 |

## Business Rules

1. Sumber data berasal dari `document_history_list` dengan filter:

   * `document_code IN ('RM21a_K','RM21b_K','RM21c_K','RM101')`

2. Data asesmen risiko jatuh berasal dari:

   * `RM21a_K`
   * `RM21b_K`
   * `RM21c_K`

3. Data intervensi pencegahan pasien jatuh berasal dari:

   * `RM101`

4. Seluruh data asesmen dari RM21A, RM21B, dan RM21C digabung menggunakan:

   ```sql
   UNION ALL
   ```

5. Perhitungan skor dilakukan sesuai instrumen masing-masing formulir.

6. Kategori risiko dinormalisasi menjadi:

   * Risiko Rendah
   * Risiko Sedang
   * Risiko Tinggi

7. Hanya pasien yang pernah memiliki:

   ```sql
   kategori_risiko = 'Risiko Tinggi'
   ```

   yang masuk dalam indikator mutu.

8. Jumlah asesmen pasien dihitung menggunakan window function:

   ```sql
   max(rn) over(partition by no_rm)
   ```

9. Asesmen awal dinyatakan dilakukan apabila:

   ```sql
   total_asesmen >= 1
   ```

10. Asesmen ulang dinyatakan dilakukan apabila:

```sql
total_asesmen > 1
```

11. Intervensi dinyatakan dilakukan apabila pasien memiliki dokumen:

```sql
RM101
```

12. Status indikator:

```sql
Ada RM101      → Memenuhi
Tidak ada RM101 → Tidak Memenuhi
```

## Definisi Indikator Mutu

* **Nama Indikator:** Pencegahan Pasien Jatuh Risiko Tinggi
* **Jenis Indikator:** Proses
* **Target:** 100%
* **Frekuensi Pengumpulan:** Bulanan

### Numerator

Jumlah pasien dengan risiko jatuh tinggi yang mendapatkan intervensi pencegahan pasien jatuh.

### Denominator

Jumlah pasien dengan risiko jatuh tinggi yang diobservasi.

### Kriteria Sukses

Pasien dengan risiko jatuh tinggi telah dilakukan intervensi pencegahan pasien jatuh dan terdokumentasi pada formulir RM101.

| ✅ Memenuhi                                  | ❌ Tidak Memenuhi                       |
| ------------------------------------------- | -------------------------------------- |
| Pasien risiko tinggi mendapatkan intervensi | Tidak terdapat intervensi pasien jatuh |
| Terdapat dokumentasi RM101                  | Tidak terdapat dokumentasi RM101       |

### Kriteria Eksklusi

* Pasien dengan kategori **Risiko Rendah**.
* Pasien dengan kategori **Risiko Sedang**.

### Formula Capaian

```text
(Jumlah status = 'Memenuhi')
/
(Jumlah pasien risiko tinggi)
× 100%
```
