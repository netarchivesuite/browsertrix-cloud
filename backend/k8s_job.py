""" entry point for K8s crawl job which manages the stateful crawl """

import os
import asyncio
import json
import datetime
import sys

import yaml
from redis import asyncio as aioredis

from kubernetes_asyncio import client, config

from kubernetes_asyncio.utils import create_from_dict, FailToCreateError

from fastapi import FastAPI
from fastapi.templating import Jinja2Templates

from crawl_updater import CrawlUpdater
from crawls import CrawlCompleteIn, Crawl, CrawlFile

app = FastAPI()
loop = asyncio.get_running_loop()


# =============================================================================
class K8SCrawlJob:
    """ Crawl Job State """

    def __init__(self):
        config.load_incluster_config()

        self.namespace = os.environ.get("CRAWL_NAMESPACE") or "crawlers"

        self.crawl_id = "crawl-" + os.environ.get("CRAWL_ID")
        self.crawls_done_key = "crawls-done"

        self.aid = os.environ.get("ARCHIVE_ID")
        self.cid = os.environ.get("CRAWL_CONFIG_ID")
        self.userid = os.environ.get("USER_ID")
        self.is_manual = os.environ.get("RUN_MANUAL") == "1"

        self.storage_path = os.environ.get("STORE_PATH")
        self.storage_name = os.environ.get("STORE_NAME")

        self.apps_api = client.AppsV1Api()
        self.core_api = client.CoreV1Api()

        self.templates = Jinja2Templates(directory="templates")

        self.crawl_updater = CrawlUpdater()

        loop.create_task(self.init_crawl_state())

        loop.create_task(self.init_redis_watch())

    async def init_crawl_state(self):
        """ init crawl state objects from crawler.yaml """
        params = {}

        for name in os.environ:
            if name.startswith("CRAWL_"):
                params[name[len("CRAWL_") :].lower()] = os.environ.get(name)

        params["id"] = self.crawl_id

        data = self.templates.env.get_template("crawler.yaml").render(params)

        await self.create_from_yaml(client, data)

    async def init_redis_watch(self):
        """ start watching crawl stateful set redis for queued messages """

        # pylint: disable=line-too-long
        redis_url = f"redis://crawl-{self.crawl_id}-0.{self.crawl_id}.{self.namespace}.svc.cluster.local/0"

        self.cluster_redis = await aioredis.from_url(
            redis_url, encoding="utf-8", decode_responses=True
        )

        if not await self.cluster_redis.get("start_time"):
            await self.cluster_redis.set(
                "start_time",
                str(datetime.datetime.utcnow().replace(microsecond=0, tzinfo=None)),
            )

        # if await cluster_redis.get("all_done"):

        while True:
            try:
                _, value = await self.cluster_redis.blpop(
                    self.crawls_done_key, timeout=0
                )
                value = json.loads(value)
                await self.handle_crawl_file_complete(CrawlCompleteIn(**value))

            # pylint: disable=broad-except
            except Exception as exc:
                print(f"Retrying crawls done loop: {exc}")
                await asyncio.sleep(10)

    async def handle_crawl_file_complete(self, crawlcomplete):
        """ Handle crawl file completion """
        # statefulset = await self.batch_api.read_namespaced_stateful_set(
        #    name=self.crawl_id, namespace=self.namespace
        # )

        # if not statefulset:  # or job.metadata.labels["btrix.user"] != crawlcomplete.user:
        #    return None, None

        # manual = job.metadata.annotations.get("btrix.run.manual") == "1"
        # if manual and not self.no_delete_jobs and crawlcomplete.completed:
        start_time = await self.cluster_redis.get("start_time")

        crawl = self.make_crawl(crawlcomplete, start_time)

        # storage_path = job.metadata.annotations.get("btrix.def_storage_path")
        inx = None
        filename = None
        # storage_name = None
        if self.storage_path:
            inx = crawlcomplete.filename.index(self.storage_path)
            filename = (
                crawlcomplete.filename[inx:] if inx > 0 else crawlcomplete.filename
            )
            # storage_name = job.metadata.annotations.get("btrix.storage_name")

        def_storage_name = self.storage_name if inx else None

        crawl_file = CrawlFile(
            def_storage_name=def_storage_name,
            filename=filename or crawlcomplete.filename,
            size=crawlcomplete.size,
            hash=crawlcomplete.hash,
        )

        await self.crawl_updater.store_crawl(self.cluster_redis, crawl, crawl_file)

        if crawlcomplete.completed:
            loop.create_task(self.delete_crawl_objects())

    def make_crawl(self, crawlcomplete, start_time):
        """ Create crawl object for partial or fully complete crawl """
        return Crawl(
            id=self.crawl_id,
            state="complete" if crawlcomplete.completed else "partial_complete",
            # scale=replicas,
            userid=self.userid,
            aid=self.aid,
            cid=self.cid,
            manual=self.is_manual,
            started=start_time,
            # watchIPs=watch_ips or [],
            # colls=json.loads(job.metadata.annotations.get("btrix.colls", [])),
            finished=datetime.datetime.utcnow().replace(microsecond=0, tzinfo=None),
        )

    async def create_from_yaml(self, k8s_client, doc):
        """ init k8s objects from yaml """
        yml_document_all = yaml.safe_load_all(doc)
        api_exceptions = []
        k8s_objects = []
        for yml_document in yml_document_all:
            try:
                created = await create_from_dict(
                    k8s_client, yml_document, verbose=False, namespace=self.namespace
                )
                k8s_objects.append(created)
            except FailToCreateError as failure:
                api_exceptions.extend(failure)

        if api_exceptions:
            raise FailToCreateError(api_exceptions)

        return k8s_objects

    async def delete_crawl_objects(self):
        statefulset = await self.apps_api.read_namespaced_stateful_set(
            name=self.crawl_id, namespace=self.namespace
        )

        if not statefulset:
            return False

        await self.core_api.delete_namespaced_service(
            name=statefulset.spec.service_name,
            namespace=self.namespace,
            propagation_policy="Foreground",
        )

        await self.apps_api.delete_namespaced_stateful_set(
            name=self.crawl_id,
            namespace=self.namespace,
            propagation_policy="Foreground",
        )

        return True

    async def exit_in(self, sec, status=0):
        await asyncio.sleep(sec)
        sys.exit(status)

    async def scale_to(self, size):
        """ scale to "size" replicas """
        statefulset = await self.apps_api.read_namespaced_stateful_set(
            name=self.crawl_id,
            namespace=self.namespace,
        )

        if not statefulset:
            return False

        statefulset.spec.replicas = size

        await self.apps_api.patch_namespaced_stateful_set(
            name=self.crawl_id, namespace=self.namespace, body=statefulset
        )

        return True


# ============================================================================
@app.on_event("startup")
async def startup():
    """init on startup"""
    job = K8SCrawlJob()

    @app.post("/scale/{size}")
    async def scale(size: int):
        return job.scale_to(size)

    @app.post("/cancel")
    async def cancel():
        await job.delete_crawl_objects()
        loop.create_task(job.exit_in(10))
        return {"success": True}

    @app.get("/healthz")
    async def healthz():
        return {}
