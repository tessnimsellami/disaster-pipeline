Voici ton `README.md` propre, prêt à **copier-coller directement** (format Markdown clean, sans bruit) 👇

---

```markdown
# 🌍 Disaster Pipeline - Data Engineering End-to-End

> Pipeline de collecte, transformation et visualisation de données de catastrophes naturelles depuis les APIs GDACS et NASA EONET.

[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://www.python.org)
[![Airflow](https://img.shields.io/badge/Airflow-2.8.0-green)](https://airflow.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.7.4-orange)](https://www.getdbt.com)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.32.0-red)](https://streamlit.io)
[![Tests](https://img.shields.io/badge/Tests-6%20passed-brightgreen)](tests/test_transformations.py)
[![Deployed](https://img.shields.io/badge/Deployed-Streamlit%20Cloud%20+%20Neon-brightgreen)](https://streamlit.io)

---

## 📋 Table des matières

- [Description](#-description)
- [Architecture](#-architecture)
- [Sources de données](#-sources-de-données)
- [Stack technologique](#-stack-technologique)
- [Installation locale](#-installation-locale)
- [Déploiement Cloud](#-déploiement-cloud)
- [Tests unitaires](#-tests-unitaires)
- [Structure du projet](#-structure-du-projet)

---

## 🎯 Description

Ce projet démontre la maîtrise d'un pipeline Data Engineering complet, de l'ingestion à la visualisation :

```

[APIs GDACS/EONET]
↓
[Kafka + MinIO] → Data Lake Bronze (raw JSON)
↓
[Airflow + dbt] → Transformations ELT
↓
[PostgreSQL/Neon] → Data Warehouse Silver/Gold
↓
[Streamlit Cloud] → Dashboard interactif en production

````

### Cas d'usage

- Surveillance en temps quasi-réel des catastrophes naturelles  
- Visualisation géographique et temporelle des événements  
- Agrégation par pays, type d'événement et niveau d'alerte  
- Support décisionnel pour les organisations humanitaires  

### 🔗 Dashboard en ligne

> **Accès public** : https://disaster-pipeline-....streamlit.app  
> *498 événements • Données GDACS + NASA EONET • Mise à jour automatique*

---

## 🏗️ Architecture

```mermaid
graph LR
    subgraph "Ingestion"
        GDACS[API GDACS] -->|@hourly| Kafka1[Kafka: earthquakes-raw]
        EONET[API EONET] -->|@daily| Kafka2[Kafka: eonet-events]
    end

    subgraph "Stockage Bronze"
        Kafka1 --> Consumer[Consumer Python]
        Kafka2 --> Consumer
        Consumer --> MinIO[MinIO: bronze/gdacs/, bronze/eonet/]
    end

    subgraph "Transformation"
        MinIO --> Airflow[Airflow DAGs]
        Airflow -->|raw_ tables| Postgres[(PostgreSQL/Neon)]
        Airflow -->|dbt run| Postgres
        Postgres -->|staging| dbt_staging[dbt: staging/]
        Postgres -->|marts| dbt_marts[dbt: marts/]
        Postgres -->|gold| dbt_gold[dbt: gold/]
    end

    subgraph "Visualisation"
        dbt_gold --> Streamlit[Streamlit Cloud]
        Streamlit --> User[👤 Utilisateur]
    end
````

### Couches de données

| Couche    | Schéma                                     | Description                            |
| --------- | ------------------------------------------ | -------------------------------------- |
| 🥉 Bronze | `public.raw_gdacs`, `public.raw_eonet`     | Données brutes JSON depuis MinIO       |
| 🥈 Silver | `disasters_staging.*`, `disasters_marts.*` | Données nettoyées et typées (dbt)      |
| 🥇 Gold   | `disasters_gold.gold_disasters`            | Vue unifiée GDACS+EONET pour Streamlit |

---

## 📡 Sources de données

### 1. GDACS - Global Disaster Alert and Coordination System

* URL : [https://www.gdacs.org/](https://www.gdacs.org/)
* Format : GeoJSON via API REST
* Fréquence : Horaire (`@hourly`)
* Champs clés : `eventid`, `eventtype`, `countryname`, `alertlevel`, `populationaffected`

### 2. NASA EONET - Earth Observatory Natural Event Tracker

* URL : [https://eonet.gsfc.nasa.gov/](https://eonet.gsfc.nasa.gov/)
* Format : JSON via API v3
* Fréquence : Quotidienne (`@daily`)
* Champs clés : `id`, `title`, `categories`, `geometry.coordinates`, `status`

---

## 🛠️ Stack technologique

| Composant          | Technologie                  | Justification                                     |
| ------------------ | ---------------------------- | ------------------------------------------------- |
| Orchestration      | Apache Airflow 2.8.0         | Gestion des dépendances, retry, logging structuré |
| Transformation     | dbt 1.7.4 + PostgreSQL       | ELT moderne, tests de données intégrés            |
| Stockage Lake      | MinIO (S3-compatible)        | Data Lake open-source                             |
| Stockage Warehouse | PostgreSQL 15 / Neon.tech    | SQL puissant, hébergement cloud gratuit           |
| Streaming          | Apache Kafka 7.5 + Zookeeper | Buffering temps réel                              |
| Visualisation      | Streamlit 1.32 + Plotly      | Dashboard interactif                              |
| Conteneurisation   | Docker + Docker Compose      | Reproductibilité                                  |
| Tests              | pytest 8.x                   | Tests unitaires                                   |

---

## 🚀 Installation locale

### Prérequis

* Docker Desktop + Docker Compose v2+
* Python 3.11+
* 4 GB RAM minimum

### Démarrage rapide

```bash
git clone https://github.com/tessnimsellami/disaster-pipeline.git
cd disaster-pipeline

docker compose --profile core up -d
sleep 30

docker compose --profile transform up -d
sleep 60

docker compose --profile viz up -d
```

### Accès

* Airflow : [http://localhost:8080](http://localhost:8080)
* Streamlit : [http://localhost:8501](http://localhost:8501)
* MinIO : [http://localhost:9001](http://localhost:9001)
* Kafka UI : [http://localhost:8090](http://localhost:8090)

---

## ☁️ Déploiement Cloud

### Architecture

```
GitHub → Streamlit Cloud → Neon.tech
```

### Variables d'environnement

```
POSTGRES_HOST=xxx
POSTGRES_DB=neondb
POSTGRES_USER=neondb_owner
POSTGRES_PASSWORD=xxx
```

### Migration

```bash
python migrate_all.py
```

---

## 🧪 Tests unitaires

```bash
pip install pytest pandas
pytest tests/test_transformations.py -v
```

### Résultat attendu

```
6 passed
```

---

## 📁 Structure du projet

```
disaster-pipeline-main/
├── docker-compose.yml
├── requirements.txt
├── README.md
├── migrate_all.py
├── airflow/
├── dashboard/
├── dbt/
├── kafka/
├── tests/
└── docs/
```

---

## 🤝 Contribution

```bash
git checkout -b feature/ma-feature
git commit -m "feat: ajout"
git push origin feature/ma-feature
```

---

## 📄 Licence

MIT License



*Projet réalisé dans le cadre d'un projet Data Engineering*
*GitHub : [https://github.com/tessnimsellami/disaster-pipeline](https://github.com/tessnimsellami/disaster-pipeline)*

