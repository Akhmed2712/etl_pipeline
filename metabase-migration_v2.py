import requests
from metabase_api import Metabase_API
import json

URL_ASAL = "http://dev.farmagitechs.co.id:3001/"
EMAIL_ASAL = "baidarus555@gmail.com"
PASSWORD_ASAL = "12454hn121th1k"
ID_DATABASE_ASAL = 3

URL_TUJUAN = "http://12.12.1.155:3000"
EMAIL_TUJUAN = "userums@gmail.com"
PASSWORD_TUJUAN = "ums_4nalytics"
ID_DATABASE_TUJUAN = 2

DASHBOARD_ID_TUJUAN = 18  # ← ganti sesuai ID dashboard hasil migrasi terakhir

source_mb = Metabase_API(domain=URL_ASAL, email=EMAIL_ASAL, password=PASSWORD_ASAL)
target_mb = Metabase_API(domain=URL_TUJUAN, email=EMAIL_TUJUAN, password=PASSWORD_TUJUAN)
headers = {"X-Metabase-Session": target_mb.session_id, "Content-Type": "application/json"}

# ── Bangun mapping nama tabel & field ──────────────────────────────────────
def index_tables(mb, db_id):
    meta = mb.get(f"/api/database/{db_id}/metadata")
    idx = {}
    for t in meta.get('tables', []):
        tname = t['name'].lower()
        fields = {f['name'].lower(): f['id'] for f in t.get('fields', [])}
        idx[tname] = {'id': t['id'], 'fields': fields}
    return idx

src_idx = index_tables(source_mb, ID_DATABASE_ASAL)
tgt_idx = index_tables(target_mb, ID_DATABASE_TUJUAN)

table_id_map = {}
field_id_map = {}
unmapped_tables = {}
unmapped_fields = {}

for tname, src in src_idx.items():
    if tname in tgt_idx:
        tgt = tgt_idx[tname]
        table_id_map[src['id']] = tgt['id']
        for fname, src_fid in src['fields'].items():
            if fname in tgt['fields']:
                field_id_map[src_fid] = tgt['fields'][fname]
            else:
                unmapped_fields[src_fid] = f"{tname}.{fname}"
    else:
        unmapped_tables[src['id']] = tname

print(f"📊 Tabel berhasil di-map : {len(table_id_map)}")
print(f"📊 Field berhasil di-map : {len(field_id_map)}")
print(f"⚠️  Tabel TIDAK ter-map  : {len(unmapped_tables)}")
for tid, tname in unmapped_tables.items():
    print(f"   - ID {tid}: '{tname}' → tidak ada di DB tujuan")
print(f"⚠️  Field TIDAK ter-map  : {len(unmapped_fields)}")
for fid, fname in unmapped_fields.items():
    print(f"   - ID {fid}: '{fname}' → tidak ada di DB tujuan")

# ── Cek semua card di dashboard tujuan ────────────────────────────────────
print(f"\n🔍 Mengecek error tiap chart di Dashboard ID {DASHBOARD_ID_TUJUAN}...\n")

dash = target_mb.get(f"/api/dashboard/{DASHBOARD_ID_TUJUAN}")
cards = dash.get('dashcards', dash.get('ordered_cards', []))

ok_count = 0
err_count = 0

for item in cards:
    card = item.get('card')
    if not card or 'id' not in card:
        continue

    card_id = card['id']
    card_name = card.get('name', 'Untitled')

    # Ambil detail card untuk cek dataset_query
    card_detail = target_mb.get(f"/api/card/{card_id}")
    query_type = card_detail.get('dataset_query', {}).get('type', 'unknown')
    display_type = card_detail.get('display', 'unknown')

    # Jalankan query
    res = requests.post(
        f"{URL_TUJUAN.rstrip('/')}/api/card/{card_id}/query",
        headers=headers,
        json={}
    )
    data = res.json()
    status = data.get('status', 'unknown')

    if status == 'completed':
        ok_count += 1
        print(f"✅ [{card_id}] '{card_name}' ({display_type}/{query_type})")
    else:
        err_count += 1
        error_msg = data.get('error', 'no error message')
        print(f"❌ [{card_id}] '{card_name}' ({display_type}/{query_type})")
        print(f"   └─ {error_msg[:250]}")

print(f"\n{'='*60}")
print(f"✅ Berhasil : {ok_count} chart")
print(f"❌ Error    : {err_count} chart")
