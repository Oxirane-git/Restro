import csv, os, glob
from collections import Counter

# Always pick the latest qualified cafe CSV
matches = sorted(glob.glob(r'Leads\unique_emails_cafes_qualified_500_*.csv'))
if not matches:
    raise FileNotFoundError("No qualified cafe CSV found in Leads/. Run build_qualified_cafes_500.py first.")
output_file = matches[-1]
print(f"Reading: {output_file}")
with open(output_file, 'r', encoding='utf-8') as f:
    rows = list(csv.DictReader(f))

print(f'Total rows: {len(rows)}')
all_have_email = all(r.get('email') for r in rows)
print(f'All have email: {all_have_email}')

emails = [r['email'].lower() for r in rows]
print(f'Unique emails: {len(set(emails))}')

cities = Counter(r.get('city', 'unknown') for r in rows)
print(f'Cities covered: {len(cities)}')
print('Top 10 cities:')
for city, count in cities.most_common(10):
    print(f'  {city}: {count}')

print(f'\nColumn headers: {list(rows[0].keys())}')
print('\nSample rows:')
for row in rows[:3]:
    print(f'  {row["business_name"]} | {row["email"]} | {row["city"]}')
