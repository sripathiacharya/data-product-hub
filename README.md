# 📦 Data Product Hub — Operator-Driven OData Engine

Data Product Hub is a lightweight, Kubernetes-native engine for publishing data products using **OData-style REST APIs**.
It is designed for **cloud platforms**, **data spaces**, and **internal developer portals**.

## ✨ Key Features

### **Kubernetes-first design**

* Data Products are defined declaratively using a **Custom Resource Definition (CRD)**.
* A **Kopf-based Operator** reconciles DataProducts into actual runtime resources:

  * Shared or dedicated engine deployments
  * Per-product Ingress routes
  * `ConfigMap` metadata for the engine
  * (future) security, QoS, tenancy, etc.

### **OData-style API**

Each dataset is automatically exposed as:

```
/odata/<data-product-id>
```

Supports:

* `$top`, `$skip`, `$orderby`
* `$select`
* Pagination + count
* Multiple backend sources
* Join logic
* Auto-generated IDs

### **Pluggable backend engine**

* Current backend: **Parquet + DuckDB**
* Local or PVC-mounted data
* Per-product joins and rename mappings

### **Flexible deployment modes**

* **Shared engine** → Many data products served by one API engine
* **Dedicated engine** → One engine per data product (strong isolation)

---

# 🏗️ Architecture

```
       +-----------------------------+
       |     DataProduct (CRD)       |
       |  apiVersion: <group>/<ver>  |
       |  kind: DataProduct          |
       +--------------+--------------+
                      |  Reconcile
                      v
              +------------------+
              |     Operator     |
              +------------------+
               | configmap update       ┌─────────────┐
               | engine reload           │ PVC-mounted │
               | ingress creation        │   Parquet   │
               v                         └─────────────┘
       +-----------------------------------------------+
       |                Shared Engine                  |
       |  loads dataproducts.json → registry          |
       |  GET /odata/<dp> → OData endpoint            |
       +-----------------------------------------------+
```

---

# 📁 Repository Layout (New)

```
data-product-hub/
│
├── src/
│   ├── engine/           # OData engine
│   └── operator/         # CRD reconciler
│
├── charts/
│   └── data-product-hub/
│       ├── templates/    # CRD, operator, engine, ingress
│       └── values.yaml   # CRD group/name, PVC, images
│
├── examples/
│   └── southafrica-scheduled-outage/
│       ├── data-product.yaml   # example CR
│       ├── sample-data/        # parquet
│
├── Dockerfile.engine
├── Dockerfile.operator
└── README.md
```

---

# 📝 Defining a Data Product (CRD)

Example: `data-product.yaml`

```yaml
apiVersion: openness.ecostruxure.se.app/v1alpha1
kind: DataProduct
metadata:
  name: southafrica-scheduled-outage-dataset

spec:
  description: "Scheduled outage plan with joined metadata"
  deploymentMode: Shared   # or Dedicated

  api:
    path: /southafrica-scheduled-outage-dataset
    version: v1
    protocol: odata
    resource: SouthAfricaScheduledOutage

  backend:
    engine: parquet_join

    sources:
      areas:
        path: examples/southafrica-scheduled-outage/sample-data/areas.parquet
        rename:
          suburb: suburb
          city: city
          province: province
          provider: provider
          block: block

      schedule:
        path: examples/southafrica-scheduled-outage/sample-data/schedule.parquet
        rename:
          date: day
          start: start_time
          end: end_time
          stage: stage

    joins:
      - left: areas
        right: schedule
        on: [provider, block]

  entity:
    name: SouthAfricaScheduledOutage
    key_column: id
    columns:
      - name: id
        type: string
        generated: true
      - name: province
        type: string
      - name: city
        type: string
      - name: suburb
        type: string
      - name: day
        type: int
      - name: stage
        type: int

  odata:
    max_top: 1000
    default_top: 100
```

Apply it:

```bash
kubectl apply -f data-product.yaml -n data-products
```

---

# ⚙️ What the Operator Does

### **Shared mode**

1. Reads CR
2. Updates the metadata ConfigMap (`data-product-hub-metadata`)
3. Calls engine reload: `/internal/reload-config`
4. Creates Ingress:

```
/odata/southafrica-scheduled-outage-dataset
```

### **Dedicated mode**

1. Generates a Deployment + Service just for this product
2. Creates dedicated metadata ConfigMap
3. Creates dedicated Ingress
4. Deletes all these automatically on `kubectl delete dp <name>`

---

# 🚀 Running Locally (Operator-Free)

You can simulate a DataProduct without Kubernetes.

### Option A — Load a CR directly

```bash
export DP_LOCAL_CR=examples/southafrica-scheduled-outage/data-product.yaml
uvicorn engine.main:app --app-dir src --reload
```

Engine logs:

```
[local] Loaded DataProduct CR from examples/... (route=southafrica-scheduled-outage-dataset)
```

### Option B — Load a metadata JSON file

```bash
export DP_METADATA_PATH=./local-dataproducts.json
uvicorn engine.main:app --app-dir src --reload
```

---

# 🔌 Querying Data Products

### Base:

```
GET /odata/<product>
```

### Examples:

```
GET /odata/southafrica-scheduled-outage-dataset?$top=10
GET /odata/southafrica-scheduled-outage-dataset?$orderby=day desc
GET /odata/southafrica-scheduled-outage-dataset?$select=suburb,stage
```

Pagination:

```
{
  "@odata.count": 42333,
  "value": [ ... ],
  "@odata.nextLink": "/odata/...?$skip=100&$top=100"
}
```

---

# 🐳 Building Images

Engine:

```bash
podman build -t <registry>/data-product-hub-engine:<tag> -f Dockerfile.engine .
podman push <registry>/data-product-hub-engine:<tag>
```

Operator:

```bash
podman build -t <registry>/data-product-hub-operator:<tag> -f Dockerfile.operator .
podman push <registry>/data-product-hub-operator:<tag>
```

---

# ☸️ Helm Installation

Update `values.yaml` with:

* CRD group/name/version
* Engine image
* Operator image
* PVC name for Parquet files

Then install:

```bash
helm install data-product-hub ./charts/data-product-hub -n data-products
```

Upgrade:

```bash
helm upgrade data-product-hub ./charts/data-product-hub -n data-products
```

---

# 🧹 Deleting a Data Product

```
kubectl delete dp southafrica-scheduled-outage-dataset -n data-products
```

Operator will:

* Remove entry from shared metadata ConfigMap
* Or delete dedicated engine resources
* Delete Ingress
* Trigger engine reload

---

# 📚 Roadmap

* `$filter` implementation
* AuthZ via CRD (`spec.security`)
* QoS policies (`spec.qos`)
* Optional caching layer
* Async streaming mode
* Schema inference + validation
