# S2TM – fg_agg_mutu_fornas

| Target Table | Target Column           | Target Data Type | Source Table                            | Source Column                                 | Transformation / Logic                                    | Join Type | Join Key             | Notes                            |
| ------------ | ----------------------- | ---------------: | --------------------------------------- | --------------------------------------------- | --------------------------------------------------------- | --------- | -------------------- | -------------------------------- |
| dc_penjualan | id_penjualan            |           BIGINT | dc_penjualan                            | id                                            | Direct mapping                                            | —         | —                    | Primary key transaksi penjualan  |
| dc_penjualan | tanggal_penjualan       |             DATE | dc_penjualan                            | tanggal_penjualan                             | Direct mapping                                            | —         | —                    | Tanggal transaksi                |
| dc_penjualan | waktu                   |        TIMESTAMP | dc_penjualan                            | waktu                                         | Direct mapping                                            | —         | —                    | Waktu transaksi                  |
| Derived      | jenis_layanan           |     VARCHAR(255) | dc_layanan_pendaftaran, dc_spesialisasi | jenis, nama                                   | `CASE WHEN jenis='Poliklinik' THEN concat('Poli ', nama)` | LEFT      | id_unit_layanan = id | Jenis layanan pasien             |
| Aggregated   | total_item              |           BIGINT | dc_detail_penjualan                     | *                                             | `count(*)`                                                | —         | —                    | Total item obat                  |
| Aggregated   | total_fornas            |           BIGINT | fg_dim_pengemasan_barang                | fornas                                        | `sum(CASE WHEN fornas='ya' THEN 1 END)`                   | —         | —                    | Total item FORNAS                |
| Aggregated   | total_non_fornas        |           BIGINT | fg_dim_pengemasan_barang                | fornas                                        | `sum(CASE WHEN fornas='tidak' THEN 1 END)`                | —         | —                    | Total item non-FORNAS            |
| Derived      | persen_fornas           |     DECIMAL(5,2) | total_fornas, total_item                | Derived                                       | `round(total_fornas*100/total_item,2)`                    | —         | —                    | Persentase FORNAS                |
| Derived      | status                  |      VARCHAR(50) | Derived                                 | persen_fornas, is_eksklusi, total_null_fornas | Penentuan status mutu                                     | —         | —                    | Memenuhi/Tidak Memenuhi/Eksklusi |
| Derived      | keterangan              |     VARCHAR(255) | dc_group_users                          | nama                                          | Keterangan eksklusi atau master obat                      | —         | —                    | Informasi tambahan               |
| Aggregated   | nama_obat_diluar_fornas |             TEXT | fg_dim_pengemasan_barang                | nama_barang                                   | `collect_set()` obat non-FORNAS                           | —         | —                    | Daftar obat non-FORNAS           |
| Aggregated   | nama_obat_fornas_null   |             TEXT | fg_dim_pengemasan_barang                | nama_barang                                   | `collect_set()` FORNAS NULL                               | —         | —                    | Obat tanpa keterangan FORNAS     |
| fg_dim_waktu | tahun                   |              INT | fg_dim_waktu                            | tahun                                         | Join dim waktu                                            | LEFT      | tanggal              | Tahun observasi                  |
| fg_dim_waktu | triwulan                |       VARCHAR(5) | fg_dim_waktu                            | triwulan                                      | `concat('Q', triwulan)`                                   | LEFT      | tanggal              | Triwulan observasi               |
| fg_dim_waktu | nama_bulan              |      VARCHAR(20) | fg_dim_waktu                            | nama_bulan                                    | Direct mapping                                            | LEFT      | tanggal              | Nama bulan                       |
| Generated    | partition_date          |             DATE | dc_penjualan                            | tanggal_penjualan                             | `to_date(tanggal_penjualan)`                              | —         | —                    | Partisi data                     |

## Business Rules

1. Data penjualan obat berasal dari:

   * `dc_penjualan`
   * `dc_detail_penjualan`

2. Hanya item dengan kategori:

   ```sql
   kategori = 'Obat'
   ```

3. Status FORNAS diperoleh dari master:

   ```sql
   fg_dim_pengemasan_barang
   ```

4. Jenis layanan ditentukan dengan aturan:

   ```sql
   CASE
     WHEN dlp.jenis = 'Poliklinik'
       THEN CONCAT('Poli ', ds.nama)
     WHEN dlp.jenis IS NOT NULL
       THEN dlp.jenis
     ELSE dp.jenis
   END
   ```

5. Total item FORNAS dihitung apabila:

   ```sql
   lower(trim(fornas)) = 'ya'
   ```

6. Total item non-FORNAS dihitung apabila:

   ```sql
   lower(trim(fornas)) != 'ya'
   AND fornas IS NOT NULL
   ```

7. Persentase FORNAS dihitung dengan formula:

   ```sql
   persen_fornas =
   total_fornas * 100 / total_item
   ```

8. Status **Eksklusi** apabila resep disetujui oleh:

   ```sql
   group user mengandung
   'komite' atau 'direktur'
   ```

9. Status **Tidak Ada Keterangan FORNAS** apabila terdapat obat yang belum memiliki informasi FORNAS pada master obat.

10. Status **Memenuhi** apabila:

```sql
persen_fornas >= 80
```

11. Status **Tidak Memenuhi** apabila:

```sql
persen_fornas < 80
```

## Definisi Indikator Mutu

* **Nama Indikator:** Kepatuhan Penggunaan Formularium Nasional (FORNAS)
* **Jenis Indikator:** Proses
* **Target:** ≥ 80%
* **Frekuensi Pengumpulan:** Bulanan

### Numerator

Jumlah transaksi/resep obat dengan persentase penggunaan obat FORNAS ≥ 80%.

### Denominator

Jumlah seluruh transaksi/resep obat yang diobservasi (tidak termasuk eksklusi).

### Kriteria Sukses

Penggunaan obat FORNAS mencapai minimal 80% dari total item obat yang diresepkan.

| ✅ Memenuhi                              | ❌ Tidak Memenuhi                  |
| --------------------------------------- | --------------------------------- |
| Persentase FORNAS ≥ 80%                 | Persentase FORNAS < 80%           |
| Seluruh obat memiliki keterangan FORNAS | Terdapat obat tanpa status FORNAS |

### Kriteria Eksklusi

* Resep yang disetujui oleh Komite Medik.
* Resep yang disetujui oleh Direktur.

### Formula Capaian

```text
(Jumlah status = 'Memenuhi')
/
(Jumlah status != 'Eksklusi')
× 100%
```
