from metabase_api import Metabase_API

URL_ASAL = "http://dev.farmagitechs.co.id:3001/"
EMAIL_ASAL = "baidarus555@gmail.com"
PASSWORD_ASAL = "12454hn121th1k"
ID_DATABASE_ASAL = 3

URL_TUJUAN = "http://12.12.1.155:3000"
EMAIL_TUJUAN = "userums@gmail.com"
PASSWORD_TUJUAN = "ums_4nalytics"
ID_DATABASE_TUJUAN = 2

source_mb = Metabase_API(domain=URL_ASAL, email=EMAIL_ASAL, password=PASSWORD_ASAL)
target_mb = Metabase_API(domain=URL_TUJUAN, email=EMAIL_TUJUAN, password=PASSWORD_TUJUAN)

print("=" * 60)
print("DB ASAL — semua tabel & field:")
print("=" * 60)
meta_src = source_mb.get(f"/api/database/{ID_DATABASE_ASAL}/metadata")
for t in sorted(meta_src.get('tables', []), key=lambda x: x['name']):
    print(f"\n[{t['id']}] {t['name']}")
    for f in sorted(t.get('fields', []), key=lambda x: x['name']):
        print(f"    [{f['id']}] {f['name']} ({f.get('base_type','')})")

print("\n" + "=" * 60)
print("DB TUJUAN — semua tabel & field:")
print("=" * 60)
meta_tgt = target_mb.get(f"/api/database/{ID_DATABASE_TUJUAN}/metadata")
for t in sorted(meta_tgt.get('tables', []), key=lambda x: x['name']):
    print(f"\n[{t['id']}] {t['name']}")
    for f in sorted(t.get('fields', []), key=lambda x: x['name']):
        print(f"    [{f['id']}] {f['name']} ({f.get('base_type','')})")
