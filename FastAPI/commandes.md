Installer gke-gcloud-auth-plugin:
gcloud components install gke-gcloud-auth-plugin

Connexion à gcloud:
gcloud auth login

Maj de l'image:
docker build -t europe-west1-docker.pkg.dev/sincere-nirvana-458912-s1/fastapi-poei/fastapi-poei:latest .
docker push europe-west1-docker.pkg.dev/sincere-nirvana-458912-s1/fastapi-poei/fastapi-poei:latest
kubectl rollout restart deployment fastapi-deployment