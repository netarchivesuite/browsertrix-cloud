""" Swarn Runner """
import os

from ..archives import S3Storage

from .utils import (
    get_templates_dir,
    run_swarm_stack,
    delete_swarm_stack,
    get_service_labels,
    ping_containers,
)

from ..crawlmanager import BaseCrawlManager


# ============================================================================
class SwarmManager(BaseCrawlManager):
    """ Docker Crawl Manager Interface"""

    # pylint: disable=too-many-instance-attributes,too-many-public-methods
    def __init__(self):
        super().__init__(get_templates_dir())

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

    async def _create_from_yaml(self, id_, yaml_data):
        return await self.loop.run_in_executor(
            None, run_swarm_stack, f"job-{id_}", yaml_data
        )

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
