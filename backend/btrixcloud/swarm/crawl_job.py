""" entry point for K8s crawl job which manages the stateful crawl """

import asyncio
import os

from fastapi import FastAPI

from .utils import ping_containers, get_service, scale_service

from .base_job import SwarmJobMixin
from ..crawl_job import CrawlJob


app = FastAPI()


# =============================================================================
class SwarmCrawlJob(SwarmJobMixin, CrawlJob):
    """ Crawl Job """

    def _add_extra_crawl_template_params(self, params):
        """ add extra params, if any, for crawl template """
        params["userid"] = os.environ.get("USER_ID")
        params["storage_filename"] = os.environ.get("STORE_FILENAME")
        params["storage_path"] = os.environ.get("STORE_PATH")

    async def _set_replicas(self, crawl, scale):
        loop = asyncio.get_running_loop()

        # if making scale smaller, ensure existing crawlers saved their data
        if scale < self._get_replicas(crawl):
            # must ping all containers as we don't know which ones will be shut down
            await loop.run_in_executor(None, ping_containers, self.crawler, "SIGUSR1")

        return await loop.run_in_executor(None, scale_service, self.crawler, scale)

    @property
    def crawler(self):
        """ get crawl service id """
        return f"crawl-{self.job_id}_crawler"

    @property
    def redis(self):
        """ get redis service id """
        return f"crawl-{self.job_id}_redis"

    def _get_replicas(self, crawl):
        return crawl.spec.mode["Replicated"]["Replicas"]

    async def _get_crawl(self):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, get_service, self.crawler)

    async def _send_shutdown_signal(self, graceful=True):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            ping_containers,
            self.crawler,
            "SIGABRT" if not graceful else "SIGINT",
        )

    # pylint: disable=line-too-long
    @property
    def redis_url(self):
        return f"redis://{self.redis}/0"


# ============================================================================
@app.on_event("startup")
async def startup():
    """init on startup"""
    job = SwarmCrawlJob()
    job.register_handlers(app)
