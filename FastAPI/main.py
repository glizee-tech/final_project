from fastapi import FastAPI, HTTPException, Query
from databricks import sql
import os

app = FastAPI()

def get_connection():
    try:
        return sql.connect(
            server_hostname=os.environ["DATABRICKS_HOST"],
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_TOKEN"]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/")
def read_root():
    return {"message": "It works"}


@app.get("/supplier_scores")
def get_supplier_scores_yearly(
    year: int = Query(default=None),
    supplier_id: int = Query(default=None)
):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Base query
        query = "SELECT * FROM workspace.final_project.gold_supplier_scores_yearly"
        filters = []
        params = []

        # Ajout des filtres dynamiques
        if year is not None:
            filters.append("year = %s")
            params.append(year)

        if supplier_id is not None:
            filters.append("supplier_id = %s")
            params.append(supplier_id)

        # Construire la requête finale
        if filters:
            query += " WHERE " + " AND ".join(filters)

        query += " LIMIT 1000"

        # Exécution
        cursor.execute(query, params)

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        results = [dict(zip(columns, row)) for row in rows]

        cursor.close()
        conn.close()

        return {"data": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))