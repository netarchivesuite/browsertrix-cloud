""" Swarn Runner """

import os
import asyncio

from fastapi.templating import Jinja2Templates

from ..archives import S3Storage

from .utils import (
    get_templates_dir,
    run_swarm_stack,
    delete_swarm_stack,
    get_service_labels,
    ping_containers,
    random_suffix
)


# ============================================================================
class SwarmManager:
    """ Docker Crawl Manager Interface"""

    # pylint: disable=too-many-instance-attributes,too-many-public-methods
    def __init__(self):
        # self.client = aiodocker.Docker()

        self.crawler_image = os.environ["CRAWLER_IMAGE"]
        self.job_image = os.environ["JOB_IMAGE"]

        self.templates = Jinja2Templates(directory=get_templates_dir())

        self.loop = asyncio.get_running_loop()

        self.storages = {
            "default": S3Storage(
                name="default",
                access_key=os.environ["STORE_ACCESS_KEY"],
                secret_key=os.environ["STORE_SECRET_KEY"],
                endpoint_url=os.environ["STORE_ENDPOINT_URL"],
                access_endpoint_url=os.environ["STORE_ACCESS_ENDPOINT_URL"],
            )
        }

    async def check_storage(self, storage_name, is_default=False):
        """ check if storage_name is valid storage """
        # if not default, don't validate
        if not is_default:
            return True

        # if default, ensure name is in default storages list
        return self.storages[storage_name]

    async def get_default_storage(self, name):
        """ return default storage by name """
        return self.storages[name]

    # pylint: disable=too-many-arguments
    async def run_profile_browser(
        self,
        userid,
        aid,
        url,
        storage=None,
        storage_name=None,
        baseprofile=None,
        profile_path=None,
    ):
        """run browser for profile creation """

        # if default storage, use name and path + profiles/
        if storage:
            storage_name = storage.name
            storage_path = storage.path + "profiles/"
        # otherwise, use storage name and existing path from secret
        else:
            storage_path = ""

        await self.check_storage(storage_name)

        browserid = f"pro-{random_suffix()}"

        params = {
            "id": browserid,
            "userid": str(userid),
            "aid": str(aid),
            "job_image": self.job_image,
            "storage_name": storage_name,
            "storage_path": storage_path or "",
            "baseprofile": baseprofile or "",
            "profile_path": profile_path,
            "url": url,
        }

        data = self.templates.env.get_template("profile_job.yaml").render(params)

        await self.loop.run_in_executor(None, run_swarm_stack, f"job-{browserid}", data)

        return browserid

    async def ping_profile_browser(self, browserid):
        """ return ping profile browser """
        return await self.loop.run_in_executor(
            None,
            ping_containers,
            "name",
            f"job-{browserid}_browser",
            "SIGUSR1",
        )

    async def get_profile_browser_metadata(self, browserid):
        """ get browser profile labels """
        return await self.loop.run_in_executor(
            None, get_service_labels, f"job-{browserid}_browser"
        )

    async def delete_profile_browser(self, browserid):
        """ delete browser job, if it is a profile browser job """
        return await self.loop.run_in_executor(
            None, delete_swarm_stack, f"job-{browserid}"
        )
