# 📦 **Data Product Hub — Kubernetes-Native OData Engine**

Data Product Hub is a declarative, operator-driven way to publish datasets as **OData-style REST APIs** in Kubernetes.
Each dataset is defined as a **DataProduct CR**, and the platform takes care of:

* backend configuration
* join logic
* routing
* API exposure
* authentication (JWT + claim mapping)
* shared or dedicated runtime engine

It is designed for **data spaces**, **integration platforms**, and **enterprise data-sharing ecosystems**.

---

# ✨ **Key Features**

### **🔹 Kubernetes-first**

* Data Products defined via CRD
* Kopf-based Operator reconciles CRs into infrastructure:

  * Shared/dedicated engine deployments
  * ConfigMaps containing metadata
  * Ingress routing
  * Reload signaling for the engine

### **🔹 OData-style REST API**

Supports:

* `$top`, `$skip`, `$orderby`
* `$select`
* `$filter` (now supports AND, OR, equality, numeric comparisons)
* Pagination and `@odata.count`

### **🔹 Pluggable Backends**

Current:

* **Parquet + DuckDB** (local or PVC)

Future:

* Delta Lake
* Database connectors
* Pandas / Arrow adapters

### **🔹 Flexible Deployment Modes**

| Mode                 | Description                                                                |
| -------------------- | -------------------------------------------------------------------------- |
| **Shared engine**    | All datasets served by one engine instance                                 |
| **Dedicated engine** | Full isolation per dataset (its own engine Deployment + Service + Ingress) |

Selection is done in the CR:

```yaml
deploymentMode: Shared   # or Dedicated
```

### **🔹 Built-in Authentication**

* Optional JWT validation
* Configurable JWKS, issuer, algorithms
* Claim mapping for application identity
* Pluggable entitlements backend:

  * Static config (current)
  * Vault (future)
  * External entitlement API (future)

---

# 🏗️ **Architecture Overview**

```
            +-------------------------------+
            |     DataProduct (CRD)         |
            |  apiVersion: <group>/<ver>    |
            +---------------+---------------+
                            |  Reconciliation
                            v
                   +-------------------+
                   |     Operator      |
                   +-------------------+
       Shared Mode  |         | Dedicated Mode
                    v         v
     +------------------+   +--------------------------+
     | Shared Engine    |   | Dedicated Engine        |
     | One instance     |   | One per DataProduct     |
     +------------------+   +--------------------------+
           ^     ^             ^
           |     |             |
   ConfigMap   Reload       PVC mount
```

---

# 📁 Repository Structure

```
data-product-hub/
│
├── src/
│   ├── engine/                   # OData engine runtime
│   └── operator/                 # Kopf operator and CRD handlers
│
├── charts/
│   └── data-product-hub/
│       ├── templates/            # CRD, operator, engine, PVC, Ingress
│       ├── values.yaml           # CRD group/name, images, auth, PVC
│
├── examples/
│   └── southafrica-scheduled-outage/
│       ├── data-product.yaml
│       ├── sample-data/*.parquet
│
├── Dockerfile.engine
├── Dockerfile.operator
└── README.md
```

---

# 📝 **Defining a Data Product**

Example: `data-product.yaml`

```yaml
apiVersion: openness.ecostruxure.se.app/v1alpha1
kind: DataProduct
metadata:
  name: southafrica-scheduled-outage-dataset

spec:
  description: "Scheduled outage plan with joined metadata"
  deploymentMode: Shared     # or Dedicated

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
          province: province
          city: city
          suburb: suburb
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

### 🔹 Shared Mode

* Updates shared ConfigMap (`data-product-hub-metadata`)
* Triggers engine reload via `/internal/reload-config`
* Creates ingress route `/odata/<path>`

### 🔹 Dedicated Mode

Operator automatically:

* Creates:

  * Deployment (engine)
  * Service
  * ConfigMap (just for one DP)
  * Ingress
* Mounts the shared PVC for parquet files
* Deletes all resources on `kubectl delete dp`

---

# 🔒 Authentication (JWT)

Enable in `values.yaml`:

```yaml
auth:
  enabled: true
  jwksUrl: "https://.../jwks-keys"
  issuer: "https://..."
  algorithms: ["RS256"]
```

Engine enforces:

1. JWT signature validity
2. Issuer, audience (optional)
3. Extract application identity (e.g., `client_id`)
4. Check entitlement backend (pluggable)

---

# 🧩 Entitlements (Authorization)

### Current:

* Static configuration (allow/deny by dataset)
* Optional: dataset may specify:

```yaml
security:
  allowedClients:
    - app-123
    - app-777
```

### Future:

* HashiCorp Vault KV
* Internal org entitlement service
* Remote PDP (OPA / Cedar)

---

# 🔌 Query API Examples

```
GET /odata/southafrica-scheduled-outage-dataset?$top=5
GET /odata/southafrica-scheduled-outage-dataset?$filter=province eq 'Gauteng' and stage gt 2
GET /odata/southafrica-scheduled-outage-dataset?$orderby=day desc
```

---

# 🧪 Local Development (No Operator Needed)

Load a CR directly:

```bash
export DP_LOCAL_CR=examples/southafrica-scheduled-outage/data-product.yaml
uvicorn engine.main:app --reload --app-dir src
```

Or load via metadata JSON:

```bash
export DP_METADATA_PATH=local-metadata.json
```

---

# 🐳 Building Docker Images

Engine:

```bash
podman build -f Dockerfile.engine -t <registry>/data-product-hub-engine:latest .
```

Operator:

```bash
podman build -f Dockerfile.operator -t <registry>/data-product-hub-operator:latest .
```

Push:

```bash
podman push <registry>/data-product-hub-engine:latest
```

---

# ☸️ Helm Installation

```bash
helm install data-product-hub ./charts/data-product-hub -n data-products
```

Values supports:

* CRD group/name
* JWT configuration
* Shared PVC
* Custom storageClass
* Image pull secrets

---

# 🧹 Deleting a Data Product

```bash
kubectl delete dp southafrica-scheduled-outage-dataset -n data-products
```

Operator:

* Cleans shared metadata
* Removes ingress
* Dedicated mode → deletes deployment, service, ConfigMap
* Reloads shared engine if needed

---

# 🗺️ Roadmap

### 📌 Engine

* Full `$filter` grammar support (substring, functions)
* Streaming response mode
* Caching layer

### 📌 Operator

* Auto PVC provisioning for parquet files
* Validate CR before applying
* Live dataset status (`status.conditions`)

### 📌 Security

* Vault entitlements backend
* Per-dataset rate limiting
