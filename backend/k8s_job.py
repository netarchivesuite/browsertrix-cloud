""" entry point for K8s crawl job which manages the stateful crawl """

import os
import asyncio
import json
import datetime
import sys
import signal

import yaml

from redis import asyncio as aioredis

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

        self.crawl_updater = CrawlUpdater(self.crawl_id)

        self.crawls_done_key = "crawls-done"

        self.initial_scale = os.environ.get("INITIAL_SCALE") or "1"
        self.scale = int(self.initial_scale)

        self.api_client = ApiClient()
        self.apps_api = client.AppsV1Api(self.api_client)
        self.core_api = client.CoreV1Api(self.api_client)
        self.core_api_ws = client.CoreV1Api(api_client=WsApiClient())

        self.templates = Jinja2Templates(directory="templates")

        self.redis = None
        self.is_running = False

        # pylint: disable=line-too-long
        # self.redis_url = f"redis://{self.crawl_id}-0.{self.crawl_id}.{self.namespace}.svc.cluster.local/0"
        # self.redis_url = f"redis://redis-{self.crawl_id}.{self.namespace}.svc.cluster.local/0"
        self.redis_url = f"redis://redis-{self.crawl_id}-0.redis-{self.crawl_id}.{self.namespace}.svc.cluster.local/0"

        self.shutdown_pending = False

        loop.create_task(self.async_init())

    async def async_init(self):
        """ async init for k8s job """
        statefulset = await self._get_crawl_stateful()

        # if doesn't exist, create
        if not statefulset:
            await self.init_crawl_state()

        else:
            self.scale = statefulset.spec.replicas

            await self.init_redis()

        await self.run_redis_loop()

    async def init_crawl_state(self):
        """ init crawl state objects from crawler.yaml """
        statefulset = await self._get_crawl_stateful()

        # if already exists, don't try to recreate
        if statefulset:
            return

        start_time = str(datetime.datetime.utcnow().replace(microsecond=0, tzinfo=None))
        await self.crawl_updater.add_new_crawl(start_time)

        with open("/config/config.yaml") as fh_config:
            params = yaml.safe_load(fh_config)

        params["id"] = self.crawl_id
        params["cid"] = self.crawl_updater.cid
        params["storage_name"] = self.crawl_updater.storage_name or "default"
        params["storage_path"] = self.crawl_updater.storage_path or ""
        params["scale"] = self.initial_scale
        params["redis_url"] = self.redis_url
        data = self.templates.env.get_template("crawler.yaml").render(params)

        await create_from_yaml(self.api_client, data, namespace=self.namespace)

        await self.init_redis()

        await self.redis.set("start_time", start_time)

    async def init_redis(self):
        """ init redis, wait for valid connection """
        retry = 10
        start_time = None

        while True:
            try:
                self.redis = await aioredis.from_url(
                    self.redis_url, encoding="utf-8", decode_responses=True
                )
                start_time = await self.redis.get("start_time")
                print("Redis Connected!", flush=True)
                break
            except:
                print(f"Retrying redis connection in {retry}", flush=True)
                await asyncio.sleep(retry)

        return start_time

    async def run_redis_loop(self):
        """ list for queued event messages in a loop. handle file add and done events """

        start_time = await self.redis.get("start_time")

        while True:
            try:
                result = await self.redis.blpop(self.crawls_done_key, timeout=5)
                if result:
                    msg = json.loads(result[1])
                    # add completed file
                    if msg.get("filename"):
                        await self.crawl_updater.add_file_to_crawl(msg, start_time)

                # update stats
                await self.crawl_updater.update_running_crawl_stats(
                    self.redis, self.crawl_id
                )

                # check crawl status
                await self.check_crawl_status()

            # pylint: disable=broad-except
            except Exception as exc:
                print(f"Retrying crawls done loop: {exc}")
                await asyncio.sleep(10)

    async def check_crawl_status(self):
        """ check if crawl is done if all crawl workers have set their done state """
        results = await self.redis.hvals(f"{self.crawl_id}:status")

        # check if done
        done = 0
        for result in results:
            if result == "done":
                done += 1

        # check if done
        if done >= self.scale:
            await self.delete_crawl_objects()

            sys.exit(0)

        # check if running
        elif not self.is_running:
            if len(results) > 0:
                print("set state to running", flush=True)
                await self.crawl_updater.update_state("running", False)
                self.is_running = True

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
            print("Delete failed", exc, flush=True)

    async def scale_to(self, size):
        """ scale to "size" replicas """
        statefulset = await self._get_crawl_stateful()

        if not statefulset:
            return False

        statefulset.spec.replicas = size
        self.scale = size

        await self.apps_api.patch_namespaced_stateful_set(
            name=self.crawl_id, namespace=self.namespace, body=statefulset
        )

        return True

    async def _get_crawl_stateful(self):
        try:
            return await self.apps_api.read_namespaced_stateful_set(
                name=self.crawl_id,
                namespace=self.namespace,
            )
        except:
            return None

    async def shutdown(self, graceful=False):
        """ shutdown crawling, either graceful or immediately"""
        if self.shutdown_pending:
            return

        self.shutdown_pending = True

        pods = await self.core_api.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=f"crawl={self.crawl_id},role=crawler",
        )

        await send_signal_to_pods(
            self.core_api_ws,
            self.namespace,
            pods.items,
            graceful,
        )

        if not graceful:
            await self.crawl_updater.update_state("canceled", finished=True)

            await self.delete_crawl_objects()
            sys.exit(0)

        else:
            await self.crawl_updater.update_state("stopping", finished=False)


# ============================================================================
@app.on_event("startup")
async def startup():
    """init on startup"""
    job = K8SCrawlJob()

    def sig_handler(sig, *_args):
        # gradual shutdown
        if sig == signal.SIGINT:
            print("got SIGINT, interrupting")
            loop.create_task(job.shutdown(graceful=False))

        elif sig == signal.SIGABRT:
            print("got SIGABRT, aborting")
            loop.create_task(job.shutdown(graceful=False))

        elif sig == signal.SIGTERM:
            print("got SIGTERM, shutting down")
            if not job.shutdown_pending:
                sys.exit(3)

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGABRT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    @app.post("/scale/{size}")
    async def scale(size: int):
        return job.scale_to(size)

    @app.get("/healthz")
    async def healthz():
        return {}
