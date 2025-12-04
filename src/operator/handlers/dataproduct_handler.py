# src/operator/handlers/dataproduct.py
import datetime
import json
import os
from typing import Any, Dict, List, Optional

import kopf
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

GROUP = os.getenv("DATA_PRODUCT_CRD_GROUP", "sripathiacharya.github.io")
VERSION = os.getenv("DATA_PRODUCT_CRD_VERSION", "v1alpha1")
PLURAL = os.getenv("DATA_PRODUCT_CRD_PLURAL", "dataproducts")

# Shared engine / ingress defaults (from env / Helm)
SHARED_ENGINE_SERVICE = os.getenv("SHARED_ENGINE_SERVICE", "data-product-hub-engine")
SHARED_ENGINE_PORT = int(os.getenv("SHARED_ENGINE_PORT", "8000"))
ENGINE_RELOAD_PATH = os.getenv("ENGINE_RELOAD_PATH", "/internal/reload-config")
SHARED_ENGINE_DEPLOYMENT = os.getenv("SHARED_ENGINE_DEPLOYMENT", "data-product-hub-data-product-hub-engine")

INGRESS_CLASS_NAME = os.getenv("INGRESS_CLASS_NAME", "nginx")
INGRESS_BASE_HOST = os.getenv("INGRESS_BASE_HOST", "data-products.dev.yourco.com")
INGRESS_TLS_SECRET = os.getenv("INGRESS_TLS_SECRET", "data-products-tls")
INGRESS_PATH_PREFIX = os.getenv("INGRESS_PATH_PREFIX", "/odata")
INGRESS_ANNOTATIONS_JSON = os.getenv("INGRESS_ANNOTATIONS_JSON", "{}")

SHARED_METADATA_CM_NAME = os.getenv("SHARED_METADATA_CM_NAME", "data-product-hub-metadata")

DATA_ROOT_PATH = os.getenv("DATA_ROOT_PATH", "/data-product-hub-data")
DATA_PVC_NAME = os.getenv("DATA_PVC_NAME", "")

def _load_k8s_config():
    # In-cluster config if running in k8s; local config if running kopf locally
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()


def _get_ingress_annotations() -> Dict[str, str]:
    try:
        return json.loads(INGRESS_ANNOTATIONS_JSON) if INGRESS_ANNOTATIONS_JSON else {}
    except json.JSONDecodeError:
        return {}


def _dataproduct_to_metadata(spec: Dict[str, Any], name: str, namespace: str) -> Dict[str, Any]:
    """
    Map DataProduct spec to the metadata JSON used by the engine.
    This is the same shape you currently put in dataproducts.json.
    """
    api = spec.get("api", {})
    backend = spec.get("backend", {})
    entity = spec.get("entity", {})
    odata = spec.get("odata", {})

    return {
        "id": name,
        "namespace": namespace,
        "display_name": spec.get("displayName", name),
        "description": spec.get("description"),
        "owner": spec.get("owner"),
        "api": {
            "path": api.get("path", f"/{name}"),
            "protocol": api.get("protocol", "odata"),
            "resource": api.get("resource", entity.get("name")),
            "version": api.get("version", "v1"),
        },
        "backend": backend,
        "entity": entity,
        "odata": odata,
        "security": spec.get("security", {}),
        "qos": spec.get("qos", {}),
        "deployment_mode": spec.get("deploymentMode", "Shared"),
    }


# --------------------------------------------------------------------
# SHARED MODE HELPERS
# --------------------------------------------------------------------

def _bump_shared_engine_revision(namespace: str, logger) -> None:
    """
    Force a restart of the shared engine by bumping a pod template annotation
    on its Deployment. This avoids races with projected ConfigMap volumes.
    """
    if not SHARED_ENGINE_DEPLOYMENT:
        logger.warning("SHARED_ENGINE_DEPLOYMENT not set; cannot bump engine revision.")
        return

    _load_k8s_config()
    apps = client.AppsV1Api()

    # Use a simple timestamp; anything that changes the value will trigger a rollout.
    revision = datetime.datetime.utcnow().isoformat()

    body = {
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        "data-product-hub/metadata-revision": revision,
                    }
                }
            }
        }
    }

    try:
        logger.info(
            f"Bumping shared engine deployment {SHARED_ENGINE_DEPLOYMENT} "
            f"annotation data-product-hub/metadata-revision={revision}"
        )
        apps.patch_namespaced_deployment(
            name=SHARED_ENGINE_DEPLOYMENT,
            namespace=namespace,
            body=body,
        )
    except client.exceptions.ApiException as e:
        logger.error(
            f"Failed to patch shared engine deployment {SHARED_ENGINE_DEPLOYMENT}: {e}"
        )
        # Don't raise; we don't want to fail the reconcile just because restart failed.

def _update_shared_metadata(namespace: str, name: str, spec: Dict[str, Any]) -> None:
    """
    Upsert this DataProduct into the shared dataproducts.json ConfigMap.
    """
    _load_k8s_config()
    v1 = client.CoreV1Api()

    try:
        cm = v1.read_namespaced_config_map(SHARED_METADATA_CM_NAME, namespace)
    except client.exceptions.ApiException as e:
        if e.status == 404:
            cm = client.V1ConfigMap(
                metadata=client.V1ObjectMeta(name=SHARED_METADATA_CM_NAME, namespace=namespace),
                data={"dataproducts.json": "[]"},
            )
            v1.create_namespaced_config_map(namespace, cm)
        else:
            raise

    existing_raw = cm.data.get("dataproducts.json", "[]")
    try:
        items: List[Dict[str, Any]] = json.loads(existing_raw)
    except json.JSONDecodeError:
        items = []

    items = [item for item in items if item.get("id") != name]

    items.append(_dataproduct_to_metadata(spec, name, namespace))

    cm.data["dataproducts.json"] = json.dumps(items, indent=2)
    v1.patch_namespaced_config_map(SHARED_METADATA_CM_NAME, namespace, cm)


def _remove_from_shared_metadata(namespace: str, name: str) -> None:
    _load_k8s_config()
    v1 = client.CoreV1Api()

    try:
        cm = v1.read_namespaced_config_map(SHARED_METADATA_CM_NAME, namespace)
    except client.exceptions.ApiException as e:
        if e.status == 404:
            return
        raise

    existing_raw = cm.data.get("dataproducts.json", "[]")
    try:
        items: List[Dict[str, Any]] = json.loads(existing_raw)
    except json.JSONDecodeError:
        items = []

    new_items = [item for item in items if item.get("id") != name]
    cm.data["dataproducts.json"] = json.dumps(new_items, indent=2)
    v1.patch_namespaced_config_map(SHARED_METADATA_CM_NAME, namespace, cm)


def _notify_engine_reload(namespace: str, service: str, port: int) -> None:
    """
    Call /internal/reload-config on the given engine Service.
    We use the cluster DNS name so this is internal-only.
    """
    import requests

    base_url = f"http://{service}.{namespace}.svc.cluster.local:{port}"
    url = base_url + ENGINE_RELOAD_PATH

    try:
        resp = requests.post(url, timeout=5)
        resp.raise_for_status()
    except Exception as exc:
        kopf.logger.error(f"Failed to reload engine at {url}: {exc}")


def _ensure_ingress_for_dp(
    namespace: str,
    name: str,
    api_path: str,
    service_name: str,
    service_port: int,
) -> None:
    """
    Create or update an Ingress per DataProduct, routing to the given service.
    """
    _load_k8s_config()
    net = client.NetworkingV1Api()

    ingress_name = f"dp-{name}"
    path = f"{INGRESS_PATH_PREFIX}{api_path}"

    annotations = _get_ingress_annotations()

    body = client.V1Ingress(
        metadata=client.V1ObjectMeta(
            name=ingress_name,
            namespace=namespace,
            annotations=annotations,
        ),
        spec=client.V1IngressSpec(
            ingress_class_name=INGRESS_CLASS_NAME,
            tls=[
                client.V1IngressTLS(
                    hosts=[INGRESS_BASE_HOST],
                    secret_name=INGRESS_TLS_SECRET,
                )
            ],
            rules=[
                client.V1IngressRule(
                    host=INGRESS_BASE_HOST,
                    http=client.V1HTTPIngressRuleValue(
                        paths=[
                            client.V1HTTPIngressPath(
                                path=path,
                                path_type="Prefix",
                                backend=client.V1IngressBackend(
                                    service=client.V1IngressServiceBackend(
                                        name=service_name,
                                        port=client.V1ServiceBackendPort(number=service_port),
                                    )
                                ),
                            )
                        ]
                    ),
                )
            ],
        ),
    )

    try:
        net.read_namespaced_ingress(ingress_name, namespace)
        net.patch_namespaced_ingress(ingress_name, namespace, body)
    except client.exceptions.ApiException as e:
        if e.status == 404:
            net.create_namespaced_ingress(namespace, body)
        else:
            raise


def _delete_ingress_for_dp(namespace: str, name: str) -> None:
    _load_k8s_config()
    net = client.NetworkingV1Api()
    ingress_name = f"dp-{name}"
    try:
        net.delete_namespaced_ingress(ingress_name, namespace)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise


# --------------------------------------------------------------------
# DEDICATED MODE HELPERS
# --------------------------------------------------------------------


def _dedicated_names(name: str) -> Dict[str, str]:
    safe = name  # you may want to shorten/slugify later
    return {
        "cm": f"dp-{safe}-metadata",
        "deploy": f"dp-{safe}-engine",
        "svc": f"dp-{safe}-engine",
    }


def _ensure_dedicated_metadata(namespace: str, name: str, spec: Dict[str, Any]) -> str:
    """
    Create/update dedicated metadata ConfigMap with a single DataProduct.
    Returns the ConfigMap name.
    """
    _load_k8s_config()
    v1 = client.CoreV1Api()
    names = _dedicated_names(name)
    cm_name = names["cm"]

    metadata_list = [_dataproduct_to_metadata(spec, name, namespace)]
    data = {"dataproducts.json": json.dumps(metadata_list, indent=2)}

    body = client.V1ConfigMap(
        metadata=client.V1ObjectMeta(name=cm_name, namespace=namespace),
        data=data,
    )

    try:
        v1.read_namespaced_config_map(cm_name, namespace)
        v1.patch_namespaced_config_map(cm_name, namespace, body)
    except client.exceptions.ApiException as e:
        if e.status == 404:
            v1.create_namespaced_config_map(namespace, body)
        else:
            raise

    return cm_name


def _ensure_dedicated_engine(namespace: str, name: str, cm_name: str) -> str:
    """
    Ensure dedicated engine Deployment + Service exist for this DataProduct.
    Returns service name.
    """
    _load_k8s_config()
    apps = client.AppsV1Api()
    v1 = client.CoreV1Api()

    names = _dedicated_names(name)
    deploy_name = names["deploy"]
    svc_name = names["svc"]

    # Deployment
    labels = {
        "app.kubernetes.io/name": "data-product-hub-engine",
        "app.kubernetes.io/component": "engine",
        "data-product-hub/dataproduct": name,
    }

    # NOTE: image should match engine image; we expect it via env IMAGE_ENGINE / DEDICATED_ENGINE_IMAGE.
    engine_image = os.getenv("DEDICATED_ENGINE_IMAGE", os.getenv("ENGINE_IMAGE", "your-reg/engine:latest"))

    # -------- env vars --------
    env_vars = [
        client.V1EnvVar(name="DP_METADATA_PATH", value="/etc/data-product-hub/dataproducts.json"),
        # IMPORTANT: same data root as shared engine, comes from DATA_ROOT_PATH
        client.V1EnvVar(name="DP_REPO_ROOT", value=DATA_ROOT_PATH),
    ]

    # -------- volume mounts --------
    volume_mounts = [
        client.V1VolumeMount(name="metadata", mount_path="/etc/data-product-hub", read_only=True),
    ]

    # If a PVC is configured, mount it at DATA_ROOT_PATH
    volumes = [
        client.V1Volume(
            name="metadata",
            config_map=client.V1ConfigMapVolumeSource(
                name=cm_name,
                items=[client.V1KeyToPath(key="dataproducts.json", path="dataproducts.json")],
            ),
        )
    ]

    if DATA_PVC_NAME:
        volume_mounts.append(
            client.V1VolumeMount(
                name="data-root",
                mount_path=DATA_ROOT_PATH,
                read_only=False,  # parquet reads only, but RW is fine
            )
        )
        volumes.append(
            client.V1Volume(
                name="data-root",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=DATA_PVC_NAME
                ),
            )
        )

    container = client.V1Container(
        name="engine",
        image=engine_image,
        image_pull_policy="IfNotPresent",
        env=env_vars,
        ports=[client.V1ContainerPort(container_port=8000, name="http")],
        volume_mounts=volume_mounts,
    )

    image_pull_secret = os.getenv("IMAGE_PULL_SECRET")

    pod_spec = client.V1PodSpec(
        containers=[container],
        volumes=volumes,
        image_pull_secrets=[
            client.V1LocalObjectReference(name=image_pull_secret)
        ] if image_pull_secret else None,
    )

    pod_template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels=labels),
        spec=pod_spec,
    )

    deploy_spec = client.V1DeploymentSpec(
        replicas=1,
        selector=client.V1LabelSelector(match_labels=labels),
        template=pod_template,
    )

    deploy_body = client.V1Deployment(
        metadata=client.V1ObjectMeta(name=deploy_name, namespace=namespace),
        spec=deploy_spec,
    )

    try:
        apps.read_namespaced_deployment(deploy_name, namespace)
        apps.patch_namespaced_deployment(deploy_name, namespace, deploy_body)
    except client.exceptions.ApiException as e:
        if e.status == 404:
            apps.create_namespaced_deployment(namespace, deploy_body)
        else:
            raise

    # Service
    svc_body = client.V1Service(
        metadata=client.V1ObjectMeta(name=svc_name, namespace=namespace),
        spec=client.V1ServiceSpec(
            selector=labels,
            ports=[client.V1ServicePort(name="http", port=8000, target_port=8000)],
        ),
    )

    try:
        v1.read_namespaced_service(svc_name, namespace)
        v1.patch_namespaced_service(svc_name, namespace, svc_body)
    except client.exceptions.ApiException as e:
        if e.status == 404:
            v1.create_namespaced_service(namespace, svc_body)
        else:
            raise

    return svc_name



def _delete_dedicated_resources(namespace: str, name: str) -> None:
    _load_k8s_config()
    apps = client.AppsV1Api()
    v1 = client.CoreV1Api()
    names = _dedicated_names(name)

    for kind, delete_fn in [
        ("deployment", apps.delete_namespaced_deployment),
        ("service", v1.delete_namespaced_service),
        ("configmap", v1.delete_namespaced_config_map),
    ]:
        res_name = names["deploy"] if kind == "deployment" else names["svc"] if kind == "service" else names["cm"]
        try:
            delete_fn(res_name, namespace)
        except ApiException as e:
            if e.status == 404:
                # Already gone, fine
                continue
            # You *could* log a warning instead of blowing up:
            # logger.warning(f"Failed to delete {kind} {res_name}: {e}")
            raise

# --------------------------------------------------------------------
# KOPF HANDLERS
# --------------------------------------------------------------------


@kopf.on.create(GROUP, VERSION, PLURAL)
@kopf.on.update(GROUP, VERSION, PLURAL)
def dataproduct_create_or_update(spec, name, namespace, logger, **kwargs):
    mode = spec.get("deploymentMode", "Shared")
    api = spec.get("api", {})
    api_path = api.get("path", f"/{name}")

    logger.info(f"Reconciling DataProduct {name} in {namespace} (mode={mode})")

    if mode == "Shared":
        # Ensure shared metadata & reload shared engine
        _update_shared_metadata(namespace, name, spec)
        _bump_shared_engine_revision(namespace, logger)

        # Shared engine ingress
        _ensure_ingress_for_dp(
            namespace=namespace,
            name=name,
            api_path=api_path,
            service_name=SHARED_ENGINE_SERVICE,
            service_port=SHARED_ENGINE_PORT,
        )

        # Optionally remove any dedicated resources if mode changed
        _delete_dedicated_resources(namespace, name)

    elif mode == "Dedicated":
        # Remove from shared if previously there
        _remove_from_shared_metadata(namespace, name)

        # Dedicated metadata + engine + service
        cm_name = _ensure_dedicated_metadata(namespace, name, spec)
        svc_name = _ensure_dedicated_engine(namespace, name, cm_name)

        # Dedicated ingress
        _ensure_ingress_for_dp(
            namespace=namespace,
            name=name,
            api_path=api_path,
            service_name=svc_name,
            service_port=8000,
        )

        # Notify dedicated engine (optional; it loads config on startup anyway)
        _notify_engine_reload(namespace, svc_name, 8000)

    else:
        raise kopf.TemporaryError(f"Unknown deploymentMode: {mode}", delay=30)


@kopf.on.delete(GROUP, VERSION, PLURAL)
def dataproduct_delete(spec, name, namespace, logger, **kwargs):
    mode = spec.get("deploymentMode", "Shared")

    logger.info(f"Deleting DataProduct {name} in {namespace} (mode={mode})")

    if mode == "Shared":
        _remove_from_shared_metadata(namespace, name)
        _bump_shared_engine_revision(namespace, logger)
    elif mode == "Dedicated":
        _delete_dedicated_resources(namespace, name)

    _delete_ingress_for_dp(namespace, name)
