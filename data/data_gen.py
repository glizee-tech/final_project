import json
import random
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

# =========================================================
# CONFIG
# =========================================================
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

fake = Faker()
fake.seed_instance(SEED)

N_SUPPLIERS = 80
N_ORDERS = 600
N_INCIDENTS = 220

START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2026, 3, 10)

OUTPUT_ORDERS_JSON = "orders.json"
OUTPUT_SUPPLIERS_PARQUET = "suppliers.parquet"
OUTPUT_INCIDENTS_CSV = "incidents.csv"

PRODUCT_CATEGORIES = [
    "electronics",
    "accessories",
    "mechanical_parts",
    "medical_supply",
    "industrial_tools",
]

INCIDENT_TYPES = [
    "delivery_delay",
    "quality_issue",
    "damaged_goods",
    "missing_items",
    "documentation_problem",
]

SEVERITIES = ["low", "medium", "high", "critical"]

COUNTRIES = [
    ("Germany", "DE"),
    ("France", "FR"),
    ("United States", "US"),
    ("China", "CN"),
    ("Sweden", "SE"),
    ("Spain", "ES"),
    ("Italy", "IT"),
    ("Netherlands", "NL"),
    ("Belgium", "BE"),
]

COUNTRY_CODE_VARIANTS = {
    "DE": ["DE", "de", "Germany", "GER"],
    "FR": ["FR", "fr", "France", "FRA"],
    "US": ["US", "us", "United States", "USA"],
    "CN": ["CN", "cn", "China", "CHN"],
    "SE": ["SE", "se", "Sweden", "SWE"],
    "ES": ["ES", "es", "Spain", "ESP"],
    "IT": ["IT", "it", "Italy", "ITA"],
    "NL": ["NL", "nl", "Netherlands", "NLD"],
    "BE": ["BE", "be", "Belgium", "BEL"],
}


# =========================================================
# HELPERS
# =========================================================
def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def random_datetime_str(dt: datetime, mode: str) -> str:
    """FMT-01: formats de date multiples"""
    if mode == "iso":
        return dt.strftime("%Y-%m-%d")
    if mode == "iso_no_zero":
        return f"{dt.year}-{dt.month}-{dt.day}"
    if mode == "slash":
        return dt.strftime("%Y/%m/%d")
    if mode == "dash_fr":
        return dt.strftime("%d-%m-%Y")
    if mode == "datetime":
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    return dt.strftime("%Y-%m-%d")


def maybe_multiformat_date(dt: datetime) -> str:
    formats = ["iso", "iso_no_zero", "slash", "dash_fr", "datetime"]
    return random_datetime_str(dt, random.choice(formats))


def random_phone(country_code: str) -> str:
    if country_code == "DE":
        return f"+49 30 {random.randint(100000, 999999)}"
    if country_code == "FR":
        return f"+33 1 {random.randint(10000000, 99999999)}"
    if country_code == "US":
        return f"+1 312 555 {random.randint(1000, 9999)}"
    if country_code == "CN":
        return f"+86 755 {random.randint(100000, 999999)}"
    if country_code == "SE":
        return f"+46 8 {random.randint(100000, 999999)}"
    return fake.phone_number()


def bad_phone() -> str:
    return random.choice([
        "12345",
        "++33ABCD",
        "phone_unknown",
        "06-XX-YY",
        "not_a_phone",
    ])


def bad_email(name: str) -> str:
    username = re.sub(r"[^a-z]", "", name.lower())
    return random.choice([
        f"{username}.mail.com",
        f"{username}@mail",
        f"{username}#company.com",
        "unknown_email",
        "n/a",
    ])


def maybe_case_noise(value: str) -> str:
    """CAS-01"""
    choices = [
        value.lower(),
        value.upper(),
        value.title(),
        value,
    ]
    return random.choice(choices)


def maybe_space_noise(value: str) -> str:
    """CAS-02 + CAS-03"""
    r = random.random()
    if r < 0.33:
        return " " + value
    if r < 0.66:
        return value + " "
    return value


def make_partial_duplicate_supplier(row: dict) -> dict:
    """ID-01 / doublon partiel logique côté suppliers"""
    new_row = row.copy()
    field_to_change = random.choice(
        ["phone_number", "contact_email", "updated_at", "city"]
    )
    if field_to_change == "phone_number":
        new_row["phone_number"] = bad_phone()
    elif field_to_change == "contact_email":
        new_row["contact_email"] = bad_email(new_row.get("contact_person", "contact"))
    elif field_to_change == "updated_at":
        new_row["updated_at"] = maybe_multiformat_date(random_date(START_DATE, END_DATE))
    elif field_to_change == "city":
        new_row["city"] = maybe_case_noise(new_row["city"])
    return new_row


# =========================================================
# 1) SUPPLIERS.PARQUET
# =========================================================
suppliers = []
valid_supplier_ids = list(range(1, N_SUPPLIERS + 1))

for supplier_id in valid_supplier_ids:
    country, country_code = random.choice(COUNTRIES)
    company_name = fake.company()
    city = fake.city()
    contact_person = fake.name()

    created_at = random_date(datetime(2025, 1, 1), datetime(2025, 12, 31))
    updated_at = random_date(created_at, END_DATE)

    row = {
        "supplier_id": supplier_id,
        "supplier_name": company_name,
        "city": maybe_case_noise(city),  # CAS-01
        "country": country,
        "country_code": random.choice(COUNTRY_CODE_VARIANTS[country_code]),  # SYN-02
        "phone_number": random_phone(country_code),
        "contact_person": contact_person,
        "contact_email": fake.email(),
        "created_at": maybe_multiformat_date(created_at),   # FMT-01
        "updated_at": maybe_multiformat_date(updated_at),   # FMT-01
        # SCH-03: colonnes séparées aussi présentes
        "contact_name": contact_person.split(" ")[0] if " " in contact_person else contact_person,
        "contact_surname": contact_person.split(" ")[-1] if " " in contact_person else None,
    }
    suppliers.append(row)

# NUL-01 : NULL natif
for idx in random.sample(range(len(suppliers)), k=6):
    suppliers[idx]["contact_email"] = None

for idx in random.sample(range(len(suppliers)), k=5):
    suppliers[idx]["contact_person"] = None

# "référent null ou pas un nom"
for idx in random.sample(range(len(suppliers)), k=5):
    suppliers[idx]["contact_person"] = random.choice([None, "12345", "???", "N/A"])

# phone_number pas au bon format
for idx in random.sample(range(len(suppliers)), k=8):
    suppliers[idx]["phone_number"] = bad_phone()

# contact_email pas au bon format
for idx in random.sample(range(len(suppliers)), k=8):
    suppliers[idx]["contact_email"] = bad_email(fake.first_name())

# ID-01 : doublons exacts
exact_duplicate_suppliers = random.sample(suppliers, k=4)
suppliers.extend([row.copy() for row in exact_duplicate_suppliers])

# Doublons partiels
partial_duplicate_suppliers = random.sample(suppliers[:N_SUPPLIERS], k=4)
suppliers.extend([make_partial_duplicate_supplier(row) for row in partial_duplicate_suppliers])

df_suppliers = pd.DataFrame(suppliers)

# =========================================================
# 2) ORDERS.JSON
# =========================================================
orders = []

for i in range(N_ORDERS):
    order_id = 1000 + i
    supplier_id = random.choice(valid_supplier_ids)

    order_date = random_date(datetime(2026, 1, 1), datetime(2026, 3, 7))
    expected_date = order_date + timedelta(days=random.randint(1, 10))
    actual_date = expected_date + timedelta(days=random.randint(-2, 5))

    items = []
    for _ in range(random.randint(1, 3)):
        quantity = random.randint(1, 120)
        unit_price = round(random.uniform(5, 1500), 2)
        items.append({
            "product_category": random.choice(PRODUCT_CATEGORIES),
            "quantity": quantity,
            "unit_price": unit_price,
        })

    row = {
        "order_id": order_id,
        "supplier_id": supplier_id,
        "order_date": maybe_multiformat_date(order_date),                 # FMT-01
        "delivery_date_expected": maybe_multiformat_date(expected_date),  # FMT-01
        "delivery_date_actual": maybe_multiformat_date(actual_date),      # FMT-01
        "items": items,
    }
    orders.append(row)

# ID-01 : doublons exacts
exact_duplicate_orders = random.sample(orders, k=10)
orders.extend([json.loads(json.dumps(row)) for row in exact_duplicate_orders])

# ID-03 : identifiant manquant
for idx in random.sample(range(len(orders)), k=10):
    orders[idx]["order_id"] = random.choice([None, "", " "])

# NUL-01 : NULL natif
for idx in random.sample(range(len(orders)), k=12):
    orders[idx]["delivery_date_actual"] = None

# NUL-05 : champ items absent du schéma
for idx in random.sample(range(len(orders)), k=10):
    if "items" in orders[idx]:
        del orders[idx]["items"]

# FMT-06 : entier stocké comme float / type incohérent
for idx in random.sample(range(len(orders)), k=15):
    if isinstance(orders[idx].get("supplier_id"), int):
        orders[idx]["supplier_id"] = float(orders[idx]["supplier_id"])

for idx in random.sample(range(len(orders)), k=15):
    if isinstance(orders[idx].get("order_id"), int):
        orders[idx]["order_id"] = float(orders[idx]["order_id"])

for order in random.sample(orders, k=30):
    if "items" in order and isinstance(order["items"], list):
        for item in order["items"]:
            if "quantity" in item and random.random() < 0.5:
                item["quantity"] = float(item["quantity"])  # FMT-06

# LOG-01 : incohérence date logique (delivery_date_actual < order_date)
for idx in random.sample(range(len(orders)), k=8):
    base = random_date(datetime(2026, 2, 1), datetime(2026, 3, 1))
    orders[idx]["order_date"] = maybe_multiformat_date(base)
    orders[idx]["delivery_date_actual"] = maybe_multiformat_date(base - timedelta(days=random.randint(1, 5)))

# LOG-03 : supplier_id orphelin (référence inexistante)
for idx in random.sample(range(len(orders)), k=12):
    orders[idx]["supplier_id"] = random.choice([9999, 8888, 7777])

# =========================================================
# 3) INCIDENTS.CSV
# =========================================================
incidents = []

# On essaie de lier une partie des incidents à des commandes
valid_order_ids_for_incidents = [
    o["order_id"] for o in orders
    if o.get("order_id") not in [None, "", " "] and isinstance(o.get("order_id"), (int, float))
]

for i in range(N_INCIDENTS):
    incident_date = random_date(datetime(2026, 1, 1), END_DATE)
    supplier_id = random.choice(valid_supplier_ids)

    row = {
        "incident_type": random.choice(INCIDENT_TYPES),
        "severity": random.choice(SEVERITIES),
        "incident_date": maybe_multiformat_date(incident_date),  # FMT-01
        "supplier_id": supplier_id,
        "order_id": random.choice(valid_order_ids_for_incidents) if valid_order_ids_for_incidents else None,
        "description": fake.sentence(nb_words=8),
    }
    incidents.append(row)

# CAS-02 + CAS-03 : espaces autour de incident_type
for idx in random.sample(range(len(incidents)), k=25):
    incidents[idx]["incident_type"] = maybe_space_noise(incidents[idx]["incident_type"])

df_incidents = pd.DataFrame(incidents)

# =========================================================
# EXPORT
# =========================================================

# orders.json
with open(OUTPUT_ORDERS_JSON, "w", encoding="utf-8") as f:
    json.dump(orders, f, ensure_ascii=False, indent=2)

# suppliers.parquet
# nécessite pyarrow ou fastparquet
df_suppliers.to_parquet(OUTPUT_SUPPLIERS_PARQUET, index=False)

# incidents.csv
df_incidents.to_csv(OUTPUT_INCIDENTS_CSV, index=False)

# =========================================================
# QUICK CHECK
# =========================================================
print("✅ Files generated:")
print(f" - {OUTPUT_ORDERS_JSON}: {len(orders)} records")
print(f" - {OUTPUT_SUPPLIERS_PARQUET}: {df_suppliers.shape}")
print(f" - {OUTPUT_INCIDENTS_CSV}: {df_incidents.shape}")

print("\nSample orders:")
print(json.dumps(orders[:2], ensure_ascii=False, indent=2))

print("\nSample suppliers:")
print(df_suppliers.head(5).to_string(index=False))

print("\nSample incidents:")
print(df_incidents.head(5).to_string(index=False))