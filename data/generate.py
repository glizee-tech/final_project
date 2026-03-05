import pandas as pd
import numpy as np
from datetime import datetime, timedelta

np.random.seed(42)

# -------------------------
# PARAMETERS
# -------------------------

N_SUPPLIERS = 50
N_ORDERS = 6000
N_INCIDENTS = 1000

suppliers = [f"Fournisseur_{i}" for i in range(1, N_SUPPLIERS + 1)]

categories = [
    "alimentaire",
    "non_alimentaire",
    "equipement_logistique"
]

incident_types = [
    "retard_livraison",
    "defaut_qualite",
    "probleme_logistique"
]

gravite_levels = ["faible", "moyenne", "critique"]

start_date = datetime(2023,1,1)

# -------------------------
# GENERATE ORDERS
# -------------------------

orders = []

for i in range(N_ORDERS):

    fournisseur = np.random.choice(suppliers)

    date_commande = start_date + timedelta(days=np.random.randint(0,730))

    delai_prevu = np.random.randint(2,15)

    delai_reel = delai_prevu + np.random.randint(-2,8)

    prix = round(np.random.uniform(10,500),2)

    quantite = np.random.randint(1,200)

    categorie = np.random.choice(categories)

    # -------- OUTLIERS --------

    if np.random.rand() < 0.01:
        prix = prix * np.random.randint(10,50)  # extreme price

    if np.random.rand() < 0.01:
        quantite = quantite * np.random.randint(10,30)  # extreme quantity

    if np.random.rand() < 0.01:
        delai_reel = delai_reel + np.random.randint(30,120)  # huge delay

    # -------- MISSING VALUES --------

    if np.random.rand() < 0.05:
        prix = np.nan

    if np.random.rand() < 0.05:
        quantite = np.nan

    if np.random.rand() < 0.03:
        categorie = np.nan

    if np.random.rand() < 0.05:
        delai_reel = np.nan

    orders.append([
        fournisseur,
        date_commande,
        delai_prevu,
        delai_reel,
        prix,
        quantite,
        categorie
    ])

df_orders = pd.DataFrame(
    orders,
    columns=[
        "fournisseur",
        "date_commande",
        "delai_prevu",
        "delai_reel",
        "prix",
        "quantite",
        "categorie_produit"
    ]
)

# -------------------------
# GENERATE INCIDENTS
# -------------------------

incidents = []

for i in range(N_INCIDENTS):

    fournisseur = np.random.choice(suppliers)

    date = start_date + timedelta(days=np.random.randint(0,730))

    type_incident = np.random.choice(incident_types)

    gravite = np.random.choice(
        gravite_levels,
        p=[0.6,0.3,0.1]
    )

    # -------- OUTLIER INCIDENT --------

    if np.random.rand() < 0.02:
        gravite = "critique"

    # -------- MISSING VALUES --------

    if np.random.rand() < 0.05:
        gravite = np.nan

    incidents.append([
        fournisseur,
        date,
        type_incident,
        gravite
    ])

df_incidents = pd.DataFrame(
    incidents,
    columns=[
        "fournisseur",
        "date",
        "type_incident",
        "gravite"
    ]
)

# -------------------------
# SAVE DATASETS
# -------------------------

df_orders.to_csv("commandes_fournisseurs.csv", index=False)
df_incidents.to_csv("incidents_fournisseurs.csv", index=False)

print("Datasets generated")
print(df_orders.shape)
print(df_incidents.shape)

print("\nMissing values in orders:")
print(df_orders.isna().sum())

print("\nMissing values in incidents:")
print(df_incidents.isna().sum())