""" entry point for K8s crawl job which manages the stateful crawl """

import os
import asyncio
import sys
import signal

import yaml

from kubernetes_asyncio import client, config
from kubernetes_asyncio.stream import WsApiClient
from kubernetes_asyncio.client.api_client import ApiClient

from fastapi import FastAPI
from fastapi.templating import Jinja2Templates

from crawl_updater import CrawlUpdater
from utils import create_from_yaml, send_signal_to_pods

app = FastAPI()
loop = asyncio.get_running_loop()


# =============================================================================
# pylint: disable=too-many-instance-attributes,bare-except,broad-except
class K8SCrawlJob:
    """ Crawl Job State """

    def __init__(self):
        config.load_incluster_config()

        self.namespace = os.environ.get("CRAWL_NAMESPACE") or "crawlers"

        self.crawl_id = os.environ.get("CRAWL_ID")
        if self.crawl_id.startswith("job-"):
            self.crawl_id = self.crawl_id[4:]

        self.crawl_updater = CrawlUpdater(self.crawl_id, self)

        self.api_client = ApiClient()
        self.apps_api = client.AppsV1Api(self.api_client)
        self.core_api = client.CoreV1Api(self.api_client)
        self.core_api_ws = client.CoreV1Api(api_client=WsApiClient())

        self.templates = Jinja2Templates(directory="templates")

        # pylint: disable=line-too-long
        # self.redis_url = f"redis://{self.crawl_id}-0.{self.crawl_id}.{self.namespace}.svc.cluster.local/0"
        # self.redis_url = f"redis://redis-{self.crawl_id}.{self.namespace}.svc.cluster.local/0"
        self.redis_url = f"redis://redis-{self.crawl_id}-0.redis-{self.crawl_id}.{self.namespace}.svc.cluster.local/0"

        self.shutdown_pending = False

        loop.create_task(self.async_init())

    async def async_init(self):
        """ async init for k8s job """
        statefulset = await self._get_crawl_stateful()
        scale = None

        # if doesn't exist, create
        if not statefulset:
            await self.init_crawl_state()
        else:
            scale = statefulset.spec.replicas

        await self.crawl_updater.init_crawl_updater(self.redis_url, scale)

    async def init_crawl_state(self):
        """ init crawl state objects from crawler.yaml """
        statefulset = await self._get_crawl_stateful()

        # if already exists, don't try to recreate
        if statefulset:
            return

        with open("/config/config.yaml") as fh_config:
            params = yaml.safe_load(fh_config)

        params["id"] = self.crawl_id
        params["cid"] = self.crawl_updater.cid
        params["storage_name"] = self.crawl_updater.storage_name or "default"
        params["storage_path"] = self.crawl_updater.storage_path or ""
        params["scale"] = str(self.crawl_updater.scale)
        params["redis_url"] = self.redis_url
        data = self.templates.env.get_template("crawler.yaml").render(params)

        await create_from_yaml(self.api_client, data, namespace=self.namespace)

    async def delete_crawl_objects(self):
        """ delete crawl stateful sets, services and pvcs """
        kwargs = {
            "namespace": self.namespace,
            "label_selector": f"crawl={self.crawl_id}",
        }

        # await self.core_api.delete_collection_namespaced_service(**kwargs)
        # await self.apps_api.delete_collection_namespaced_stateful_set(**kwargs)

        statefulsets = await self.apps_api.list_namespaced_stateful_set(**kwargs)

        for statefulset in statefulsets.items:
            print(f"Deleting service {statefulset.spec.service_name}")
            await self.core_api.delete_namespaced_service(
                name=statefulset.spec.service_name,
                namespace=self.namespace,
                propagation_policy="Foreground",
            )
            print(f"Deleting statefulset {statefulset.metadata.name}")
            await self.apps_api.delete_namespaced_stateful_set(
                name=statefulset.metadata.name,
                namespace=self.namespace,
                propagation_policy="Foreground",
            )

        # until delete policy is supported
        try:
            await self.core_api.delete_collection_namespaced_persistent_volume_claim(
                **kwargs
            )
        except Exception as exc:
            print("PVC Delete failed", exc, flush=True)

        print("Crawl objects deleted, crawl job complete, exiting", flush=True)
        sys.exit(0)

    async def scale_to(self, scale):
        """ scale to 'scale' replicas """
        statefulset = await self._get_crawl_stateful()

        if not statefulset:
            print("no stateful")
            return False

        # if making scale smaller, ensure existing crawlers saved their data
        pods = []
        for inx in range(scale, statefulset.spec.replicas):
            pods.append(
                await self.core_api.read_namespaced_pod(
                    name=f"crawl-{self.crawl_id}-{inx}",
                    namespace=self.namespace,
                )
            )

        print("pods", len(pods))
        if pods:
            await send_signal_to_pods(self.core_api_ws, self.namespace, pods, "SIGUSR1")

        statefulset.spec.replicas = scale

        await self.apps_api.patch_namespaced_stateful_set(
            name=statefulset.metadata.name, namespace=self.namespace, body=statefulset
        )

        await self.crawl_updater.update_crawl(scale=scale)

        return True

    async def _get_crawl_stateful(self):
        try:
            return await self.apps_api.read_namespaced_stateful_set(
                name=f"crawl-{self.crawl_id}",
                namespace=self.namespace,
            )
        except Exception as e:
            return None

    async def shutdown(self, graceful=False):
        """ shutdown crawling, either graceful or immediately"""
        if self.shutdown_pending:
            return False

        self.shutdown_pending = True

        print("Stopping crawl" if graceful else "Canceling crawl", flush=True)

        pods = await self.core_api.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=f"crawl={self.crawl_id},role=crawler",
        )

        await send_signal_to_pods(
            self.core_api_ws,
            self.namespace,
            pods.items,
            "SIGABRT" if not graceful else "SIGINT",
        )

        await self.crawl_updater.stop_crawl(graceful=graceful)

        if not graceful:
            await self.delete_crawl_objects()

        return True


# ============================================================================
@app.on_event("startup")
async def startup():
    """init on startup"""
    job = K8SCrawlJob()

    def sig_handler(sig, *_args):
        if sig == signal.SIGTERM:
            print("got SIGTERM, job not complete, but shutting down", flush=True)
            if not job.shutdown_pending:
                sys.exit(3)

    signal.signal(signal.SIGTERM, sig_handler)

    @app.post("/scale/{size}")
    async def scale(size: int):
        return {"success": await job.scale_to(size)}

    @app.post("/stop")
    async def stop():
        return {"success": await job.shutdown(graceful=True)}

    @app.post("/cancel")
    async def cancel():
        return {"success": await job.shutdown(graceful=False)}

    @app.get("/healthz")
    async def healthz():
        return {}
