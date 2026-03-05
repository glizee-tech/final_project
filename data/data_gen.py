import numpy as np
import pandas as pd
from faker import Faker

# =========================
# CONFIG
# =========================
SEED = 42
np.random.seed(SEED)

fake = Faker("fr_FR")
fake.seed_instance(SEED)

N_SUPPLIERS = 80
N_ORDERS = 25000
START_DATE = "2024-01-01"
END_DATE = "2025-12-31"

CATEGORIES = ["alimentaire", "non_alimentaire", "logistique"]
INCIDENT_TYPES = ["retard_livraison", "defaut_qualite", "probleme_logistique"]
SEVERITIES = ["faible", "moyen", "critique"]

# Data quality for Silver layer
MISSING_RATE = 0.02
DUPLICATE_RATE = 0.008
OUTLIER_RATE = 0.003

# Saisonnalité (poids par mois)
MONTH_WEIGHTS = {
    1: 0.90, 2: 0.85, 3: 0.95, 4: 0.95,
    5: 1.00, 6: 1.10, 7: 1.15, 8: 1.10,
    9: 1.00, 10: 1.05, 11: 1.20, 12: 1.25
}

# Choc de marché (ex: hausse prix non alimentaire en 2025-03 et 2025-04)
SHOCK_CATEGORY = "non_alimentaire"
SHOCK_START = "2025-03-01"
SHOCK_END = "2025-04-30"
SHOCK_MULTIPLIER = 1.18  # +18% pendant le choc

# =========================
# HELPERS
# =========================
def clip(a, lo, hi):
    return np.minimum(np.maximum(a, lo), hi)

def make_date_pool(start, end):
    return pd.date_range(start=start, end=end, freq="D")

def seasonal_sample_dates(n, start, end):
    """Échantillonne des dates avec saisonnalité via poids mensuels."""
    dates = make_date_pool(start, end)
    months = dates.month.values
    w = np.array([MONTH_WEIGHTS[m] for m in months], dtype=float)
    w = w / w.sum()
    sampled = np.random.choice(dates, size=n, replace=True, p=w)
    return pd.to_datetime(sampled)

def logistic_skew_supplier_probs(suppliers, heavy_share=0.35, heavy_frac=0.15):
    """
    Crée une distribution où une minorité de fournisseurs reçoit une grosse part des commandes.
    heavy_frac des fournisseurs reçoivent heavy_share des commandes.
    """
    n = len(suppliers)
    heavy_n = max(1, int(n * heavy_frac))
    heavy_suppliers = np.random.choice(suppliers, size=heavy_n, replace=False)

    probs = np.zeros(n, dtype=float)
    idx = {s: i for i, s in enumerate(suppliers)}

    heavy_p = heavy_share / heavy_n
    rest_share = 1.0 - heavy_share
    rest_n = n - heavy_n
    rest_p = rest_share / max(1, rest_n)

    for s in suppliers:
        probs[idx[s]] = heavy_p if s in heavy_suppliers else rest_p

    probs = probs * np.random.uniform(0.95, 1.05, size=n)
    probs = probs / probs.sum()
    return probs, set(heavy_suppliers)

def normalize_company(name: str) -> str:
    if len(name) < 10 or " " not in name:
        suffix = np.random.choice(["Distribution", "Logistics", "Trading", "Supply", "Services"])
        return f"{name} {suffix}"
    return name

def make_unique_company_names(n, fake_obj):
    names = set()
    while len(names) < n:
        names.add(normalize_company(fake_obj.company()))
    return list(names)
# =========================
# 1) SUPPLIERS + RISK PROFILE
# =========================
# Bronze-friendly: une seule colonne 'fournisseur' avec des noms réalistes
suppliers = make_unique_company_names(N_SUPPLIERS, fake)

# Profil risque (0 bon, 1 moyen, 2 risqué)
risk_profile = np.random.choice([0, 1, 2], size=N_SUPPLIERS, p=[0.60, 0.25, 0.15])
supplier_risk = dict(zip(suppliers, risk_profile))

# Volatilité prix et propension au retard
supplier_price_vol = {
    s: (0.03 if supplier_risk[s] == 0 else 0.08 if supplier_risk[s] == 1 else 0.18)
    for s in suppliers
}
supplier_delay_lambda = {
    s: (0.4 if supplier_risk[s] == 0 else 1.2 if supplier_risk[s] == 1 else 2.8)
    for s in suppliers
}

# DÉPENDANCE FOURNISSEUR: distribution skewed
supplier_probs, heavy_suppliers = logistic_skew_supplier_probs(
    suppliers, heavy_share=0.35, heavy_frac=0.15
)

# =========================
# 2) ORDERS DATA (ERP)
# =========================
df_orders = pd.DataFrame({
    "fournisseur": np.random.choice(suppliers, size=N_ORDERS, p=supplier_probs),
    "date_commande": seasonal_sample_dates(N_ORDERS, START_DATE, END_DATE),
    "categorie_produit": np.random.choice(CATEGORIES, size=N_ORDERS, p=[0.45, 0.40, 0.15])
})

# délais prévus selon catégorie
base_delai_prevu = {
    "alimentaire": (2, 10),
    "non_alimentaire": (5, 20),
    "logistique": (3, 15),
}
df_orders["delai_prevu"] = [
    np.random.randint(base_delai_prevu[cat][0], base_delai_prevu[cat][1] + 1)
    for cat in df_orders["categorie_produit"]
]

# retards corrélés au fournisseur + saison (plus de retards en Nov/Dec)
retards = []
for s, d in zip(df_orders["fournisseur"], df_orders["date_commande"]):
    lam = supplier_delay_lambda[s]

    # saison: fin d'année = plus de charge logistique
    if d.month in [11, 12]:
        lam *= 1.25
    elif d.month in [7, 8]:
        lam *= 1.10

    r = np.random.poisson(lam=lam)

    # beaucoup de 0
    if np.random.rand() < 0.55:
        r = 0

    # parfois en avance
    if np.random.rand() < 0.08:
        r = -np.random.randint(1, 4)

    retards.append(r)

df_orders["delai_reel"] = (df_orders["delai_prevu"] + np.array(retards)).astype(int)
df_orders["delai_reel"] = clip(df_orders["delai_reel"], 0, 60).astype(int)

# PRIX: base par catégorie + inflation + choc + volatilité fournisseur
base_price = {"alimentaire": 3.0, "non_alimentaire": 15.0, "logistique": 8.0}

days_from_start = (df_orders["date_commande"] - pd.to_datetime(START_DATE)).dt.days.values
inflation = 1.0 + (days_from_start / max(1, days_from_start.max())) * 0.06  # +6%

shock_start = pd.to_datetime(SHOCK_START)
shock_end = pd.to_datetime(SHOCK_END)

prices = []
for s, cat, dt, infl in zip(df_orders["fournisseur"], df_orders["categorie_produit"], df_orders["date_commande"], inflation):
    mu = base_price[cat] * infl

    # choc de marché sur une catégorie
    if cat == SHOCK_CATEGORY and shock_start <= dt <= shock_end:
        mu *= SHOCK_MULTIPLIER

    vol = supplier_price_vol[s]

    # lognormal -> positif + volatilité
    p = np.random.lognormal(mean=np.log(mu), sigma=vol)

    # fournisseurs “dominants” peuvent négocier légèrement mieux (prix un peu plus bas)
    if s in heavy_suppliers:
        p *= np.random.uniform(0.97, 0.995)

    prices.append(p)

df_orders["prix"] = np.round(prices, 2)

# Quantité: lognormal + catégorie influence
q = np.random.lognormal(mean=2.2, sigma=0.65, size=N_ORDERS)
q = q * np.where(df_orders["categorie_produit"].values == "logistique", 1.35, 1.0)
df_orders["quantite"] = clip(q.astype(int), 1, 800)

# =========================
# 3) DATA QUALITY ISSUES
# =========================
for col in ["prix", "quantite", "delai_reel"]:
    mask = np.random.rand(N_ORDERS) < MISSING_RATE
    df_orders.loc[mask, col] = np.nan

mask_out = np.random.rand(N_ORDERS) < OUTLIER_RATE
df_orders.loc[mask_out, "prix"] = df_orders.loc[mask_out, "prix"] * np.random.choice([4, 6, 8], size=mask_out.sum())

mask_out_q = np.random.rand(N_ORDERS) < OUTLIER_RATE
df_orders.loc[mask_out_q, "quantite"] = df_orders.loc[mask_out_q, "quantite"] * np.random.choice([5, 10], size=mask_out_q.sum())

n_dups = int(N_ORDERS * DUPLICATE_RATE)
if n_dups > 0:
    dup_rows = df_orders.sample(n=n_dups, random_state=SEED)
    df_orders = pd.concat([df_orders, dup_rows], ignore_index=True)

df_orders["date_commande"] = pd.to_datetime(df_orders["date_commande"])

# =========================
# 4) INCIDENTS (corrélés aux retards)
# =========================
tmp = df_orders.dropna(subset=["delai_reel", "delai_prevu"]).copy()
tmp["is_late"] = (tmp["delai_reel"] > tmp["delai_prevu"]).astype(int)
late_ratio = tmp.groupby("fournisseur")["is_late"].mean().to_dict()

orders_by_supplier = df_orders["fournisseur"].value_counts().to_dict()

incident_rows = []
for s in suppliers:
    risk = supplier_risk[s]
    n_orders_s = orders_by_supplier.get(s, 0)
    lr = late_ratio.get(s, 0.0)

    base_rate = 0.0025 if risk == 0 else 0.009 if risk == 1 else 0.022
    corr_boost = 1.0 + (lr * (2.2 if risk == 2 else 1.6 if risk == 1 else 1.2))

    lam = max(0.2, n_orders_s * base_rate * corr_boost)
    expected_incidents = int(np.random.poisson(lam=lam))

    if expected_incidents == 0:
        continue

    supplier_order_dates = df_orders.loc[df_orders["fournisseur"] == s, "date_commande"].dropna()
    if len(supplier_order_dates) > 0:
        base_dates = np.random.choice(supplier_order_dates.values, size=expected_incidents, replace=True)
        inc_dates = pd.to_datetime(base_dates) + pd.to_timedelta(np.random.randint(0, 11, size=expected_incidents), unit="D")
    else:
        inc_dates = seasonal_sample_dates(expected_incidents, START_DATE, END_DATE)

    for d in inc_dates:
        p_retard = clip(0.25 + 0.90 * lr, 0.30, 0.75)
        remaining = 1.0 - p_retard
        p_qualite = remaining * 0.65
        p_log = remaining * 0.35

        t = np.random.choice(INCIDENT_TYPES, p=[p_retard, p_qualite, p_log])

        if risk == 0:
            g = np.random.choice(SEVERITIES, p=[0.78, 0.20, 0.02])
        elif risk == 1:
            g = np.random.choice(SEVERITIES, p=[0.58, 0.32, 0.10])
        else:
            g = np.random.choice(SEVERITIES, p=[0.38, 0.40, 0.22])

        incident_rows.append({"type_incident": t, "gravite": g, "date": d, "fournisseur": s})

df_incidents = pd.DataFrame(incident_rows)

if len(df_incidents) > 0:
    mask = np.random.rand(len(df_incidents)) < 0.01
    df_incidents.loc[mask, "date"] = pd.NaT

# =========================
# 5) EXPORT
# =========================
df_orders = df_orders.sort_values("date_commande").reset_index(drop=True)
df_incidents["date"] = pd.to_datetime(df_incidents["date"])
df_incidents = df_incidents.sort_values("date").reset_index(drop=True)

df_orders.to_csv("commandes_fournisseurs.csv", index=False)
df_incidents.to_csv("incidents_fournisseurs.csv", index=False)

print("✅ Fichiers générés :")
print(" - commandes_fournisseurs.csv :", df_orders.shape)
print(" - incidents_fournisseurs.csv :", df_incidents.shape)

print("\nInfos quick-check :")
print(" - Nb fournisseurs heavy (dépendance):", len(heavy_suppliers))
print(" - Exemples heavy:", sorted(list(heavy_suppliers))[:5])

print("\nAperçu commandes:")
print(df_orders.head(5))

print("\nAperçu incidents:")
print(df_incidents.head(5))

print("\n--- Data quality quick checks ---")
print("Orders - null rates:\n", df_orders[["prix","quantite","delai_reel"]].isna().mean())
print("Orders - approx duplicates:", df_orders.duplicated().mean())
print("Incidents - null rate date:", df_incidents["date"].isna().mean())