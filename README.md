# 📦 Data Product Hub (Mini OData Engine)

**Data Product Hub** is a lightweight data-serving engine that lets you expose curated datasets using **YAML configuration**, **Parquet files**, and an **OData-style REST API**.

It supports:

* 🧩 **Multiple data products**
* 🔗 **Joins across backend sources** (e.g., `areas + schedule`)
* 📁 **Data stored as Parquet files**
* 📜 **Declarative YAML config** for each product
* 🛠️ **OData-style API**: `$select`, `$top`, `$skip`, `$orderby`, pagination
* 📊 **Raw source access** (e.g., `/areas`, `/schedule`)
* 🐳 **Docker + Podman builds**
* ☸️ **Helm chart for Kubernetes deployment**

---

## ✨ How it Works

Each data product has:

1. A YAML configuration
2. One or more Parquet data sources
3. A route at `/odata/{product}`
4. Optional joins between sources

The engine loads all configs on startup and publishes each dataset as a REST data API.

---

# 🗂️ Project Structure

```
data-product-hub/
│
├── src/
│   ├── data_product_hub/
│   │   ├── main.py
│   │   ├── ...
│   └── odata/
│       ├── router.py
│       ├── registry.py
│       └── ...
│
├── config/
│   └── data-products/
│       └── southafrica-scheduled-outage.yaml
│
├── sample-data/
│   └── south-africa-outages/
│       ├── areas.parquet
│       └── schedule.parquet
│
├── tests/
│   └── test_southafrica_product.py
│
├── charts/
│   └── data-product-hub/
│       ├── templates/
│       ├── values.yaml
│       └── Chart.yaml
│
├── Dockerfile
└── README.md
```

---

# 📝 Example Data Product Config (YAML)

`config/data-products/southafrica-scheduled-outage.yaml`

```yaml
id: southafrica-scheduled-outage-dataset
route: southafrica-scheduled-outage-dataset
description: SA outage dataset from areas × schedule join

entity:
  name: Outage

backend:
  sources:
    areas:
      path: areas.parquet
      format: parquet
    schedule:
      path: schedule.parquet
      format: parquet
  
  joins:
    - left: areas
      right: schedule
      on:
        - provider
        - block

odata:
  default_top: 100
  max_top: 1000
```

---

# 🚀 Running Locally

## 1️⃣ Install dependencies

```sh
pip install -r requirements.txt
```

(or, if using pyproject)

```sh
pip install .
```

---

## 2️⃣ Run using Uvicorn

```sh
python -m uvicorn data_product_hub.main:app --reload --app-dir src
```

You should see:

```
Loaded config southafrica-scheduled-outage-dataset
join: areas -> schedule on ['provider','block']
```

Open:

👉 [http://127.0.0.1:8000/odata/southafrica-scheduled-outage-dataset](http://127.0.0.1:8000/odata/southafrica-scheduled-outage-dataset)
👉 [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

---

# 🧪 Running Tests

Tests live under `/tests`.

Run them with:

```sh
python -m pytest
```

If missing:

```sh
pip install pytest httpx
```

---

# 🌐 OData-Style API

The engine exposes classic OData query parameters.

## Available parameters

| Parameter  | Meaning                               |
| ---------- | ------------------------------------- |
| `$select`  | Column projection                     |
| `$top`     | Page size (auto-clamped by `max_top`) |
| `$skip`    | Offset                                |
| `$orderby` | Sort ascending/descending             |
| `$filter`  | (accepted but ignored for now)        |

---

## 🧩 Joined Dataset Endpoint

```
GET /odata/{product}
```

Example:

```
GET /odata/southafrica-scheduled-outage-dataset?$top=5
```

Response:

```json
{
  "@odata.context": "/odata/$metadata#southafrica-scheduled-outage-dataset",
  "@odata.count": 42333,
  "value": [ ...5 rows... ],
  "@odata.nextLink": "/odata/southafrica-scheduled-outage-dataset?$skip=5&$top=5"
}
```

---

## 🗄️ Raw Backend Source Access

```
GET /odata/{product}/{source}
```

Example:

```
GET /odata/southafrica-scheduled-outage-dataset/areas?$top=10
```

Identical response structure:

```json
{
  "@odata.context": "/odata/$metadata#southafrica-scheduled-outage-dataset/areas",
  "@odata.count": 12000,
  "value": [ ... ],
  "@odata.nextLink": "/odata/...?$skip=10&$top=10"
}
```

---

# 🐳 Running using Docker or Podman

## Build

```sh
docker build -t data-product-hub .
```

Or in Podman:

```sh
podman build -t data-product-hub .
```

## Run

```sh
docker run -p 8000:8000 data-product-hub
```

(or)

```sh
podman run -p 8000:8000 data-product-hub
```

---

# ☸️ Deploy on Kubernetes (Helm)

We include a Helm chart under:

```
charts/data-product-hub/
```

## Install

```sh
helm install data-hub charts/data-product-hub -f charts/data-product-hub/values.yaml
```

## Includes:

* Deployment
* Service
* ConfigMap for product configs
* PersistentVolumeClaim (sample-data)
* Ingress (path-based routing)

You can place all YAML configs under:

```
charts/data-product-hub/config/data-products/
```

and auto-load them via Helm.

---

# 🔌 Configuration Directory Logic

The engine loads configs from:

1. `$CONFIG_DIR` (if provided)

```
export CONFIG_DIR=/app/config/data-products
```

2. Else default:

```
data_product_hub/config/data-products
```

This allows:

* Helm ConfigMap mounts
* Docker COPY
* Local development

without conflicts.

---

# 📚 Roadmap (coming soon)

* `$filter` implementation
* Service document (`/odata/`)
* More OData-compliant metadata document
* Optional pagination token strategy
* Optional column type inference / schema enforcement

