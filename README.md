# final_project

Projet de data pipeline + API (FastAPI) avec notebooks d’exploration/traitement.

## Structure

- `pipeline/`: exécution du pipeline
- `utils/`: fonctions utilitaires
- `data/`: scripts liés aux données (ex: upload)
- `FastAPI/`: API FastAPI (docker/k8s)
- `notebook/`: notebooks (bronze/silver/gold, setup, landing)

## Prérequis

- Python 3.10+ recommandé

## Installation (pipeline)

```bash
pip install -r requirements.txt
```

## Lancer le pipeline

```bash
python pipeline/pipeline.py
```

## Lancer l’API FastAPI

```bash
pip install -r FastAPI/requirements.txt
python FastAPI/main.py
```

## Données

- `orders.json`, `incidents.csv`, `suppliers.parquet`: exemples de datasets utilisés par le projet.

