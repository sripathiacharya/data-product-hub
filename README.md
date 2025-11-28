# 📘 **README — Data Product Hub**

A lightweight, config-driven framework for exposing **data products** through an **OData-style query API** using **FastAPI**, **Pandas**, and simple **YAML configuration**.

This project allows you to:

* Load datasets (Parquet, CSV, etc.)
* Define transformations and joins using YAML
* Expose the combined dataset as a **data product**
* Support OData-like features:

  * `$filter`
  * `$select`
  * `$top / $skip`
  * `$orderby`

No code needed for new data products — **just configuration**.

---

# 🚀 Features

### ✔ Config-driven

Each data product is defined entirely in YAML under:

```
config/data-products/
```

### ✔ Automatic OData endpoint

Every product becomes accessible at:

```
/odata/{product_id}
```

### ✔ Dynamic schema discovery

OData metadata is available at:

```
/odata/$metadata
```

### ✔ YAML → Pandas → FastAPI pipeline

Uses simple YAML:

```yaml
backend:
  engine: parquet_join
  sources:
    areas:
      path: sample-data/south-africa-outages/areas.parquet
    schedule:
      path: sample-data/south-africa-outages/schedule.parquet

  joins:
    - left: areas
      right: schedule
      "on": ["provider", "block"]
```

---

# 📂 Project Structure

```
data-product-hub/
│
├── pyproject.toml            ← Project dependencies & metadata
├── README.md
│
├── src/
│   ├── data_product_hub/
│   │   ├── main.py
│   │   └── __init__.py
│   │
│   └── odata/
│       ├── router.py
│       ├── registry.py
│       ├── filter.py
│       └── __init__.py
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
└── tests/
    └── test_southafrica_product.py
```

---

# 🏁 Getting Started

## 1. Install dependencies

```
pip install -e .
```

or, if using `uv`:

```
uv sync
```

---

## 2. Run the development server

From project root:

```bash
python -m uvicorn data_product_hub.main:app --reload --app-dir src
```

Server runs at:

```
http://127.0.0.1:8000
```

---

# 🔎 Using the API

## 1. List available data products

```
GET /odata/$metadata
```

Example response:

```json
[
  {
    "id": "southafrica-scheduled-outage-dataset",
    "route": "southafrica-scheduled-outage-dataset",
    "description": "Scheduled outage plan for South African suburbs...",
    "entity": "SouthAfricaScheduledOutage"
  }
]
```

---

## 2. Query a dataset

### Full dataset

```
GET /odata/southafrica-scheduled-outage-dataset
```

### Filter by city + province

```
GET /odata/southafrica-scheduled-outage-dataset?
  $filter=province eq 'Eastern Cape' and city eq 'Amahlathi'
```

### Select only certain columns

```
?$select=province,city,suburb,stage,start_time,end_time
```

### Pagination

```
?$top=50&$skip=100
```

---

# ⚙️ Creating a New Data Product

Add a YAML file under:

```
config/data-products/<product-id>.yaml
```

Define:

* backend engine
* sources (CSV/Parquet paths)
* joins
* rename/mapping rules
* entity fields

Example:

```yaml
id: my-product
route: my-product
description: My custom dataset.

backend:
  engine: parquet_join

  sources:
    base:
      path: sample-data/mydata.parquet

entity:
  key_column: id
  properties:
    - name: id
      type: string
    - name: value
      type: string
```

Restart server — the new endpoint appears automatically.

---

# 🧪 Running Tests

```
pytest
```

Tests include:

* Product registration
* Metadata exposure
* OData query validation
* Basic filtering and projection

---

# 🧱 Philosophy

This project is built on **two simple ideas**:

1. **Data products should not require code**
   YAML defines the dataset and joins.

2. **APIs must be generic and reusable**
   One engine, many products.

---

# 📌 Future Enhancements (optional)

* Relationships & `$expand`
* Composite keys
* Query pushdown into DuckDB
* Dataset versioning
* Publishing metadata to GAIA-X / EnergyData-X

---

# 🙌 Contributing

PRs welcome!
All code lives under `src/`, tests under `tests/`.
