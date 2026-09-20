# Image commune aux services Python de la maquette : Airflow (orchestration),
# MLflow (registre de modèles) et le simulateur applicatif (sources).
FROM apache/airflow:2.9.3-python3.11

# Répertoires des volumes nommés, créés au nom de l'utilisateur "airflow" :
# un volume vide hérite du propriétaire du répertoire de l'image, ce qui évite
# de faire tourner MLflow ou le simulateur en root.
USER root
RUN mkdir -p /data/app_events /mlflow \
    && chown -R airflow:root /data /mlflow
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
