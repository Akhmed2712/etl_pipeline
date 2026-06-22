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

print("🔄 Menghubungkan ke kedua server Metabase...")
source_mb = Metabase_API(domain=URL_ASAL, email=EMAIL_ASAL, password=PASSWORD_ASAL)
target_mb = Metabase_API(domain=URL_TUJUAN, email=EMAIL_TUJUAN, password=PASSWORD_TUJUAN)
headers_tujuan = {"X-Metabase-Session": target_mb.session_id, "Content-Type": "application/json"}
print("✅ Koneksi berhasil!")

# ==========================================
# BANGUN TABLE & FIELD ID MAPPING OTOMATIS
# ==========================================
print("🗺️ Membangun peta ID tabel dan field antara DB asal & tujuan...")

def get_tables(mb, db_id):
    res = mb.get(f"/api/database/{db_id}/metadata")
    return res.get('tables', [])

src_tables = get_tables(source_mb, ID_DATABASE_ASAL)
tgt_tables = get_tables(target_mb, ID_DATABASE_TUJUAN)

# Map: nama_tabel -> {id, fields: {nama_field -> field_id}}
def index_tables(tables):
    idx = {}
    for t in tables:
        tname = t.get('name', '').lower()
        fields = {}
        for f in t.get('fields', []):
            fields[f.get('name', '').lower()] = f['id']
        idx[tname] = {'id': t['id'], 'fields': fields}
    return idx

src_idx = index_tables(src_tables)
tgt_idx = index_tables(tgt_tables)

# table_id_map: old_table_id -> new_table_id
# field_id_map: old_field_id -> new_field_id
table_id_map = {}
field_id_map = {}

for tname, src_info in src_idx.items():
    if tname in tgt_idx:
        tgt_info = tgt_idx[tname]
        table_id_map[src_info['id']] = tgt_info['id']
        # Map field IDs berdasarkan nama field
        for fname, src_fid in src_info['fields'].items():
            if fname in tgt_info['fields']:
                field_id_map[src_fid] = tgt_info['fields'][fname]

print(f"✅ Berhasil memetakan {len(table_id_map)} tabel dan {len(field_id_map)} field.")

# ==========================================
# FUNGSI REKURSIF REMAP ID DALAM QUERY
# ==========================================
def remap_query(obj):
    """Rekursif ganti semua ID lama (table & field) dalam struktur query."""
    if isinstance(obj, dict):
        new_obj = {}
        for k, v in obj.items():
            if k == 'source-table' and isinstance(v, int):
                new_obj[k] = table_id_map.get(v, v)
            elif k == 'database' and v == ID_DATABASE_ASAL:
                new_obj[k] = ID_DATABASE_TUJUAN
            else:
                new_obj[k] = remap_query(v)
        return new_obj
    elif isinstance(obj, list):
        result = []
        for i, v in enumerate(obj):
            # Format field Metabase: ["field", <id>, <options>]
            if (i == 0 and v == "field") or (i == 1 and isinstance(obj[0], str) and obj[0] == "field" and isinstance(v, int)):
                result.append(v)
            else:
                result.append(remap_query(v))
        # Khusus handle ["field", old_id, opts] → ["field", new_id, opts]
        if len(result) >= 2 and result[0] == "field" and isinstance(result[1], int):
            result[1] = field_id_map.get(result[1], result[1])
        return result
    else:
        return obj

# ==========================================
# MULAI MIGRASI
# ==========================================
print(f"📦 Mengambil data dashboard asal (ID: {DASHBOARD_ID_YANG_MAU_DIPINDAH})...")
dash_data = source_mb.get(f"/api/dashboard/{DASHBOARD_ID_YANG_MAU_DIPINDAH}")

print("📋 Membuat dashboard baru di tujuan...")
res_dash = requests.post(
    f"{URL_TUJUAN.rstrip('/')}/api/dashboard",
    headers=headers_tujuan,
    json={"name": dash_data['name'], "collection_id": ID_KOLEKSI_TUJUAN}
)
if res_dash.status_code != 200:
    raise Exception(f"Gagal membuat dashboard: {res_dash.text}")

new_dash_id = res_dash.json()['id']
print(f"✅ Dashboard baru ID: {new_dash_id}")

# Siapkan tab payload
tabs = dash_data.get('tabs', [])
tab_mapping = {}
new_tabs_payload = []
fake_tab_counter = -1

for tab in tabs:
    fake_id = fake_tab_counter
    tab_mapping[tab['id']] = fake_id
    new_tabs_payload.append({"id": fake_id, "name": tab['name']})
    fake_tab_counter -= 1

# Salin semua card
cards = dash_data.get('ordered_cards', dash_data.get('dashcards', dash_data.get('cards', [])))
print(f"🚀 Ditemukan {len(cards)} komponen...")

new_dashcards = []
fake_card_counter = -1

for index, item in enumerate(cards, 1):
    old_card = item.get('card')
    old_tab_id = item.get('dashboard_tab_id')
    new_tab_id = tab_mapping.get(old_tab_id) if old_tab_id else None

    vis_settings = item.get('visualization_settings', {}) or {}
    param_mappings = item.get('parameter_mappings', []) or []

    target_card_id = None
    component_name = "Teks/Widget"

    if old_card and 'id' in old_card:
        card_id = old_card['id']
        component_name = old_card.get('name', 'Untitled')

        try:
            card_detail = source_mb.get(f"/api/card/{card_id}")
            dataset_query = card_detail.get('dataset_query', {})

            # ✅ REMAP SEMUA ID (database, source-table, field IDs)
            dataset_query = remap_query(dataset_query)

            # table_id di level card — remap juga
            table_id = card_detail.get('table_id')
            if table_id and table_id in table_id_map:
                table_id = table_id_map[table_id]
            elif dataset_query.get('type') in ('query', 'native'):
                table_id = None

            card_vis = card_detail.get('visualization_settings') or {}

            res_card = requests.post(
                f"{URL_TUJUAN.rstrip('/')}/api/card",
                headers=headers_tujuan,
                json={
                    "name": card_detail.get('name'),
                    "description": card_detail.get('description'),
                    "display": card_detail.get('display'),
                    "visualization_settings": card_vis,
                    "dataset_query": dataset_query,
                    "collection_id": ID_KOLEKSI_TUJUAN,
                    "table_id": table_id
                }
            )

            if res_card.status_code == 200:
                target_card_id = res_card.json()['id']
                print(f"[{index}/{len(cards)}] ✅ '{component_name}' (ID: {target_card_id})")
            else:
                print(f"[{index}/{len(cards)}] ❌ Gagal: '{component_name}': {res_card.text[:150]}")
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

# PUT sekaligus
print(f"\n📌 Mengirim {len(new_tabs_payload)} tab + {len(new_dashcards)} komponen via PUT...")
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
    print(f"❌ Gagal PUT: {res_put.status_code} - {res_put.text[:300]}")

print(f"\n🎉 SELESAI! Dashboard ID {new_dash_id} di server tujuan.")
