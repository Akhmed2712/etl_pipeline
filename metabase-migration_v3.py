import requests
from metabase_api import Metabase_API

URL_ASAL = "http://dev.farmagitechs.co.id:3001/"
EMAIL_ASAL = "baidarus555@gmail.com"
PASSWORD_ASAL = "12454hn121th1k"
DASHBOARD_ID_YANG_MAU_DIPINDAH = 4

URL_TUJUAN = "http://12.12.1.155:3000"
EMAIL_TUJUAN = "userums@gmail.com"
PASSWORD_TUJUAN = "ums_4nalytics"
ID_KOLEKSI_TUJUAN = None

ID_DATABASE_ASAL = 3
ID_DATABASE_TUJUAN = 2

# ══════════════════════════════════════════════════════════════
# HARDCODED MAPPING — 100% akurat berdasarkan data nyata
# ══════════════════════════════════════════════════════════════

TABLE_ID_MAP = {
    47: 17,   # fg_agg_mutu_fornas
    54: 17,   # fg_afg_agg_mutu_fornas_bk → fg_agg_mutu_fornas
    36: 11,   # fg_dim_pengemasan_barang
    46: 16,   # fg_fact_mutu_hasil_lab_kritis
    57: 9,    # fg_fact_mutu_identifikasi_pasien
    45: 9,    # fg_fact_mutu_kepatuhan_identifikasi_pasien → fg_fact_mutu_identifikasi_pasien
    56: 10,   # fg_fact_mutu_pasien_jatuh
    40: 13,   # fg_fact_mutu_rawat_jalan
    51: 13,   # fg_rawat_jalan_bk → fg_fact_mutu_rawat_jalan
    39: 12,   # fg_fact_mutu_scp
    50: 12,   # fg_fact_mutu_scp_bk → fg_fact_mutu_scp
    41: 14,   # fg_fact_mutu_visite_dokter
    52: 14,   # fg_fact_mutu_visite_dokter_bk → fg_fact_mutu_visite_dokter
    49: 14,   # fgfg_fact_mutu_visite_dokter_bk → fg_fact_mutu_visite_dokter
    44: 15,   # fg_staging_document_history_list
}

FIELD_ID_MAP = {
    # ── fg_agg_mutu_fornas (47→17) ──────────────────────────
    383: 101, 588: 104, 590: 110, 394: 115, 591: 111, 592: 112,
    395: 116, 390: 108, 391: 109, 392: 113, 587: 102, 389: 106,
    388: 105, 589: 107, 393: 114, 387: 103,

    # ── fg_afg_agg_mutu_fornas_bk (54→17) ───────────────────
    626: 101, 629: 104, 635: 110, 640: 115, 636: 111, 637: 112,
    641: 116, 633: 108, 634: 109, 638: 113, 627: 102, 631: 106,
    630: 105, 632: 107, 639: 114, 628: 103,

    # ── fg_dim_pengemasan_barang (36→11) ────────────────────
    414: 122, 416: 124, 609: 125, 415: 123, 422: 131, 420: 129,
    421: 130, 409: 117, 411: 119, 410: 118, 418: 127, 417: 126,
    423: 132, 424: 133, 425: 134, 412: 120, 426: 135, 419: 128,
    413: 121,

    # ── fg_fact_mutu_hasil_lab_kritis (46→16) ───────────────
    586: 137, 437: 136, 445: 145, 446: 146, 441: 141, 442: 142,
    443: 143, 444: 144, 439: 139, 438: 138, 440: 140,

    # ── fg_fact_mutu_identifikasi_pasien (57→9) ─────────────
    663: 72,  668: 77,  669: 78,  675: 82,  667: 76,  664: 73,
    673: 83,  665: 74,  670: 79,  671: 80,  674: 81,  666: 75,

    # ── fg_fact_mutu_kepatuhan_identifikasi_pasien (45→9) ───
    # map ke fg_fact_mutu_identifikasi_pasien berdasarkan nama field
    447: 72,  # id → id
    460: 82,  # nama_bulan → nama_bulan
    461: 83,  # partition_date → partition_date
    457: 79,  # status → status
    458: 80,  # tahun → tahun  ← INI YANG ERROR SEBELUMNYA
    459: 81,  # triwulan → triwulan
    450: 76,  # nama → nama_verifikator (closest match)
    449: 74,  # id_pasien → pasien
    # field berikut tidak ada padanannya di tujuan → diabaikan
    # 456: id_diagnosa, 448: id_layanan_pendaftaran, 455: id_operasi
    # 454: id_penjualan, 451: nik, 452: tanggal_lahir, 453: tindakan

    # ── fg_fact_mutu_pasien_jatuh (56→10) ───────────────────
    653: 93,  654: 94,  661: 88,  644: 84,  655: 95,  650: 91,
    659: 99,  645: 85,  660: 100, 646: 86,  648: 89,  652: 96,
    657: 97,  647: 87,  651: 92,  649: 90,  658: 98,

    # ── fg_fact_mutu_rawat_jalan (40→13) ────────────────────
    462: 147, 470: 158, 559: 150, 558: 148, 662: 149, 471: 159,
    466: 154, 560: 151, 467: 155, 468: 156, 469: 157, 561: 152,
    562: 153,

    # ── fg_rawat_jalan_bk (51→13) ───────────────────────────
    563: 147, 571: 158, 572: 159, 567: 154, 568: 155, 569: 156,
    570: 157,

    # ── fg_fact_mutu_scp (39→12) ────────────────────────────
    541: 165, 539: 163, 540: 164, 472: 160, 543: 167, 544: 168,
    483: 173, 538: 162, 537: 161, 484: 174, 479: 169, 480: 170,
    481: 171, 542: 166, 482: 172,

    # ── fg_fact_mutu_scp_bk (50→12) ─────────────────────────
    545: 160, # id → id
    554: 171, # tahun → tahun
    555: 172, # triwulan → triwulan
    556: 173, # nama_bulan → nama_bulan
    557: 174, # partition_date → partition_date
    552: 169, # selisih_menit → selisih_menit
    553: 170, # status → status
    546: 162, # id_pasien → nama_pasien (closest)
    549: 166, # tanggal → tanggal_operasi (closest)

    # ── fg_fact_mutu_visite_dokter (41→14) ──────────────────
    536: 179, 485: 175, 487: 182, 573: 178, 492: 187, 585: 176,
    493: 188, 535: 177, 488: 183, 489: 184, 490: 185, 642: 180,
    643: 181, 491: 186,

    # ── fg_fact_mutu_visite_dokter_bk (52→14) ───────────────
    576: 179, # dokter
    574: 175, # id
    578: 182, # jam
    583: 187, # nama_bulan
    584: 188, # partition_date
    575: 177, # pasien
    579: 183, # pemeriksaan
    580: 184, # status
    581: 185, # tahun
    577: 180, # tanggal → tanggal_masuk
    582: 186, # triwulan

    # ── fgfg_fact_mutu_visite_dokter_bk (49→14) ─────────────
    526: 175, # id
    528: 182, # jam
    533: 187, # nama_bulan
    534: 188, # partition_date
    529: 183, # pemeriksaan
    530: 184, # status
    531: 185, # tahun
    527: 180, # tanggal → tanggal_masuk
    532: 186, # triwulan

    # ── fg_staging_document_history_list (44→15) ────────────
    516: 190, 515: 189, 520: 194, 521: 195, 518: 192, 519: 193,
    517: 191,
}

# ══════════════════════════════════════════════════════════════

print("🔄 Menghubungkan ke kedua server Metabase...")
source_mb = Metabase_API(domain=URL_ASAL, email=EMAIL_ASAL, password=PASSWORD_ASAL)
target_mb = Metabase_API(domain=URL_TUJUAN, email=EMAIL_TUJUAN, password=PASSWORD_TUJUAN)
headers_tujuan = {"X-Metabase-Session": target_mb.session_id, "Content-Type": "application/json"}
print("✅ Koneksi berhasil!")

def remap_query(obj):
    if isinstance(obj, dict):
        new_obj = {}
        for k, v in obj.items():
            if k == 'source-table' and isinstance(v, int):
                new_obj[k] = TABLE_ID_MAP.get(v, v)
            elif k == 'database' and v == ID_DATABASE_ASAL:
                new_obj[k] = ID_DATABASE_TUJUAN
            else:
                new_obj[k] = remap_query(v)
        return new_obj
    elif isinstance(obj, list):
        result = [remap_query(v) for v in obj]
        if len(result) >= 2 and result[0] == 'field' and isinstance(result[1], int):
            result[1] = FIELD_ID_MAP.get(result[1], result[1])
        return result
    return obj

def remap_template_tags(template_tags, src_table_id=None):
    if not isinstance(template_tags, dict):
        return template_tags
    new_tags = {}
    for tag_name, tag_val in template_tags.items():
        if not isinstance(tag_val, dict):
            new_tags[tag_name] = tag_val
            continue
        new_tag = dict(tag_val)
        if tag_val.get('type') == 'dimension':
            dim = tag_val.get('dimension', [])
            if (isinstance(dim, list) and len(dim) >= 2
                    and dim[0] == 'field' and isinstance(dim[1], int)):
                old_fid = dim[1]
                new_fid = FIELD_ID_MAP.get(old_fid)
                if new_fid:
                    new_tag['dimension'] = ['field', new_fid] + list(dim[2:])
                    new_tag['field-id']  = new_fid
                    print(f"      🔗 Tag '{tag_name}': {old_fid} → {new_fid}")
                else:
                    print(f"      ⚠️  Tag '{tag_name}' field {old_fid} tidak ada di map → fallback text")
                    new_tag.pop('dimension', None)
                    new_tag.pop('field-id', None)
                    new_tag['type'] = 'text'
                    new_tag.pop('widget-type', None)
        new_tags[tag_name] = new_tag
    return new_tags

# ── Migrasi ────────────────────────────────────────────────────────────────
print(f"\n📦 Mengambil dashboard asal (ID: {DASHBOARD_ID_YANG_MAU_DIPINDAH})...")
dash_data = source_mb.get(f"/api/dashboard/{DASHBOARD_ID_YANG_MAU_DIPINDAH}")

print("📋 Membuat dashboard baru...")
res_dash = requests.post(
    f"{URL_TUJUAN.rstrip('/')}/api/dashboard",
    headers=headers_tujuan,
    json={"name": dash_data['name'], "collection_id": ID_KOLEKSI_TUJUAN}
)
if res_dash.status_code != 200:
    raise Exception(f"Gagal: {res_dash.text}")
new_dash_id = res_dash.json()['id']
print(f"✅ Dashboard baru ID: {new_dash_id}")

tabs = dash_data.get('tabs', [])
tab_mapping, new_tabs_payload, fake_tab_counter = {}, [], -1
for tab in tabs:
    fid = fake_tab_counter
    tab_mapping[tab['id']] = fid
    new_tabs_payload.append({"id": fid, "name": tab['name']})
    fake_tab_counter -= 1

cards = dash_data.get('ordered_cards', dash_data.get('dashcards', dash_data.get('cards', [])))
print(f"🚀 {len(cards)} komponen ditemukan...")

new_dashcards, fake_card_counter = [], -1

for index, item in enumerate(cards, 1):
    old_card       = item.get('card')
    old_tab_id     = item.get('dashboard_tab_id')
    new_tab_id     = tab_mapping.get(old_tab_id) if old_tab_id else None
    vis_settings   = item.get('visualization_settings') or {}
    param_mappings = item.get('parameter_mappings') or []
    target_card_id = None
    component_name = "Teks/Widget"

    if old_card and 'id' in old_card:
        card_id        = old_card['id']
        component_name = old_card.get('name', 'Untitled')
        try:
            card_detail   = source_mb.get(f"/api/card/{card_id}")
            dataset_query = card_detail.get('dataset_query', {})
            query_type    = dataset_query.get('type', '')

            if dataset_query.get('database') == ID_DATABASE_ASAL:
                dataset_query['database'] = ID_DATABASE_TUJUAN

            if query_type == 'native':
                native_part  = dataset_query.get('native', {})
                src_table_id = card_detail.get('table_id')
                if 'template-tags' in native_part:
                    print(f"[{index}] 🔗 Remap tags '{component_name}'...")
                    native_part['template-tags'] = remap_template_tags(
                        native_part['template-tags'], src_table_id)
                dataset_query['native'] = native_part
                table_id = None
            elif query_type == 'query':
                dataset_query = remap_query(dataset_query)
                raw_tid  = card_detail.get('table_id')
                table_id = TABLE_ID_MAP.get(raw_tid) if raw_tid else None
            else:
                dataset_query = remap_query(dataset_query)
                table_id = None

            res_card = requests.post(
                f"{URL_TUJUAN.rstrip('/')}/api/card",
                headers=headers_tujuan,
                json={
                    "name": card_detail.get('name'),
                    "description": card_detail.get('description'),
                    "display": card_detail.get('display'),
                    "visualization_settings": card_detail.get('visualization_settings') or {},
                    "dataset_query": dataset_query,
                    "collection_id": ID_KOLEKSI_TUJUAN,
                    "table_id": table_id
                }
            )
            if res_card.status_code == 200:
                target_card_id = res_card.json()['id']
                print(f"[{index}/{len(cards)}] ✅ '{component_name}' (ID: {target_card_id})")
            else:
                print(f"[{index}/{len(cards)}] ❌ '{component_name}': {res_card.text[:200]}")
                continue
        except Exception as e:
            print(f"[{index}/{len(cards)}] ❌ Error '{component_name}': {e}")
            continue
    else:
        print(f"[{index}/{len(cards)}] 📝 Teks/Widget")

    dashcard_entry = {
        "id": fake_card_counter,
        "card_id": target_card_id,
        "size_x": item.get('size_x', 4),
        "size_y": item.get('size_y', 4),
        "row": item.get('row', 0),
        "col": item.get('col', 0),
        "parameter_mappings": param_mappings,
        "visualization_settings": vis_settings,
    }
    if new_tab_id is not None:
        dashcard_entry["dashboard_tab_id"] = new_tab_id
    new_dashcards.append(dashcard_entry)
    fake_card_counter -= 1

print(f"\n📌 PUT {len(new_tabs_payload)} tab + {len(new_dashcards)} komponen...")
put_payload = {"dashcards": new_dashcards}
if new_tabs_payload:
    put_payload["tabs"] = new_tabs_payload

res_put = requests.put(
    f"{URL_TUJUAN.rstrip('/')}/api/dashboard/{new_dash_id}",
    headers=headers_tujuan,
    json=put_payload
)

if res_put.status_code in [200, 201]:
    result = res_put.json()
    print(f"✅ {len(result.get('tabs',[]))} tab, {len(result.get('dashcards',[]))} komponen terpasang.")
else:
    print(f"❌ PUT gagal: {res_put.status_code} - {res_put.text[:300]}")

print(f"\n🎉 SELESAI! Dashboard ID {new_dash_id}")
