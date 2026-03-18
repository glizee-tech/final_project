from fastapi import FastAPI, HTTPException
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
    return {"message": "Il est 16h45"}


@app.get("/suppliers")
def get_suppliers():
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM workspace.final_project.gold_dim_suppliers LIMIT 1000")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        results = [dict(zip(columns, row)) for row in rows]

        cursor.close()
        conn.close()

        return {"data": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))