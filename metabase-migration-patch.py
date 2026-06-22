import requests
from metabase_api import Metabase_API

URL_TUJUAN = "http://12.12.1.155:3000"
EMAIL_TUJUAN = "userums@gmail.com"
PASSWORD_TUJUAN = "ums_4nalytics"
ID_DATABASE_TUJUAN = 2

# Dashboard ID terakhir hasil migrasi
DASHBOARD_ID_TUJUAN = 25  # ganti sesuai dashboard terbaru

target_mb = Metabase_API(domain=URL_TUJUAN, email=EMAIL_TUJUAN, password=PASSWORD_TUJUAN)
headers = {"X-Metabase-Session": target_mb.session_id, "Content-Type": "application/json"}

# ── Mapping field lama → baru (dari hasil diagnostik) ─────────────────────
# Tambah di sini kalau nanti ada field lain yang error
FIELD_ID_FIX = {
    458: 80,   # tahun   (fg_fact_mutu_kepatuhan_identifikasi_pasien → fg_fact_mutu_identifikasi_pasien)
    459: 81, # triwulan — uncomment kalau perlu
    460: 82, # bulan    — uncomment kalau perlu
}

# ── Ambil semua card di dashboard ─────────────────────────────────────────
print(f"🔍 Mengambil card dari Dashboard ID {DASHBOARD_ID_TUJUAN}...")
dash = target_mb.get(f"/api/dashboard/{DASHBOARD_ID_TUJUAN}")
cards = dash.get('dashcards', dash.get('ordered_cards', []))
print(f"   {len(cards)} komponen ditemukan.\n")

patched = 0
skipped = 0

for item in cards:
    card = item.get('card')
    if not card or 'id' not in card:
        continue

    card_id   = card['id']
    card_name = card.get('name', 'Untitled')

    card_detail   = target_mb.get(f"/api/card/{card_id}")
    dataset_query = card_detail.get('dataset_query', {})
    query_type    = dataset_query.get('type', '')

    if query_type != 'native':
        continue

    native_part   = dataset_query.get('native', {})
    template_tags = native_part.get('template-tags', {})

    if not template_tags:
        continue

    changed = False
    for tag_name, tag_val in template_tags.items():
        if not isinstance(tag_val, dict) or tag_val.get('type') != 'dimension':
            continue

        dim = tag_val.get('dimension', [])
        if not (isinstance(dim, list) and len(dim) >= 2
                and dim[0] == 'field' and isinstance(dim[1], int)):
            continue

        old_fid = dim[1]
        if old_fid in FIELD_ID_FIX:
            new_fid = FIELD_ID_FIX[old_fid]
            print(f"  🔧 Card [{card_id}] '{card_name}' — tag '{tag_name}': {old_fid} → {new_fid}")
            template_tags[tag_name]['dimension'] = ['field', new_fid] + list(dim[2:])
            template_tags[tag_name]['field-id']  = new_fid
            changed = True

    if not changed:
        skipped += 1
        continue

    # Update card di target
    native_part['template-tags'] = template_tags
    dataset_query['native']      = native_part

    res = requests.put(
        f"{URL_TUJUAN.rstrip('/')}/api/card/{card_id}",
        headers=headers,
        json={"dataset_query": dataset_query}
    )

    if res.status_code == 200:
        print(f"  ✅ Card [{card_id}] '{card_name}' berhasil di-patch.\n")
        patched += 1
    else:
        print(f"  ❌ Card [{card_id}] gagal: {res.status_code} - {res.text[:200]}\n")

print(f"🎉 Selesai! {patched} card di-patch, {skipped} card dilewati.")
