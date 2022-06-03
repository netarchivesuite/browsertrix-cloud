""" entry point for K8s crawl job which manages the stateful crawl """

import asyncio

from fastapi import FastAPI

from .utils import ping_containers, get_service, scale_service

from .base_job import SwarmJobMixin
from ..crawl_job import CrawlJob


app = FastAPI()


# =============================================================================
class SwarmCrawlJob(SwarmJobMixin, CrawlJob):
    """ Crawl Job """

    async def _set_replicas(self, crawl, scale):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, scale_service, "browser-{self.job_id}_browser", scale
        )

    def _get_replicas(self, crawl):
        return crawl.spec.mode["Replicated"]["Replicas"]

    async def _get_crawl(self):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, get_service, f"browser-{self.job_id}_browser"
        )

    async def _send_shutdown_signal(self, graceful=True):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            ping_containers,
            f"browser-{self.job_id}_browser",
            "SIGABRT" if not graceful else "SIGINT",
        )

    # pylint: disable=line-too-long
    @property
    def redis_url(self):
        return f"redis://crawler-{self.job_id}_redis/0"


# ============================================================================
@app.on_event("startup")
async def startup():
    """init on startup"""
    job = SwarmCrawlJob()
    job.register_handlers(app)
