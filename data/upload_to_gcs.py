from google.cloud import storage
from datetime import datetime
import os


def upload_csv_to_gcs(bucket_name, local_file, destination_path):
    """
    Upload un fichier CSV vers Google Cloud Storage
    """

    # Création du client GCS (utilise gcloud auth automatiquement)
    client = storage.Client()

    # Accéder au bucket
    bucket = client.bucket(bucket_name)

    # Créer l'objet destination
    blob = bucket.blob(destination_path)

    # Upload du fichier
    blob.upload_from_filename(local_file)

    print(f"Upload réussi : {local_file} → gs://{bucket_name}/{destination_path}")


if __name__ == "__main__":

    BUCKET_NAME = "poei-projet-achat"
    LOCAL_FILE = "commandes_fournisseurs.csv"

    today = datetime.now().strftime("%Y-%m-%d")
    DESTINATION_PATH = f"./{today}/{LOCAL_FILE}"

    upload_csv_to_gcs(BUCKET_NAME, LOCAL_FILE, DESTINATION_PATH)