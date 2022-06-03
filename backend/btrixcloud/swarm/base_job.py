""" base k8s job driver """

import os
import asyncio

# import sys
import yaml

from fastapi.templating import Jinja2Templates

from .utils import get_templates_dir, run_swarm_stack, delete_swarm_stack


# =============================================================================
# pylint: disable=too-many-instance-attributes,bare-except,broad-except
class SwarmBaseJob:
    """ Crawl Job State """

    def __init__(self):
        self.config_file = "/btrix_shared_job_config"
        self.storages_file = "/var/run/secrets/btrix_storages"
        self.curr_storage = {}

        self.job_id = os.environ.get("JOB_ID")
        self.prefix = os.environ.get("STACK_PREFIX", "stack-")

        self.templates = Jinja2Templates(directory=get_templates_dir())

        super().__init__()

    async def async_init(self, template, params):
        """ async init, overridable by subclass """
        await self.init_job_objects(template, params)

    async def init_job_objects(self, template, extra_params=None):
        """ init swarm objects from specified template with given extra_params """
        with open(self.config_file) as fh_config:
            params = yaml.safe_load(fh_config)

        params["id"] = self.job_id

        if extra_params:
            params.update(extra_params)

        if os.environ.get("STORAGE_NAME") and not self.curr_storage:
            self.load_storage(os.environ.get("STORAGE_NAME"))

        if self.curr_storage:
            params.update(self.curr_storage)

        data = self.templates.env.get_template(template).render(params)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, run_swarm_stack, self.prefix + self.job_id, data
        )

    async def delete_job_objects(self, _):
        """ remove swarm service stack """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, delete_swarm_stack, self.prefix + self.job_id)
        print("Removed other objects, removing ourselves", flush=True)
        await loop.run_in_executor(None, delete_swarm_stack, f"job-{self.job_id}")
        return True

    def load_storage(self, storage_name):
        """ load storage credentials for given storage from yaml file """
        with open(self.storages_file) as fh_config:
            data = yaml.safe_load(fh_config.read())

        if not data or not data.get("storages"):
            return

        for storage in data["storages"]:
            if storage.get("name") == storage_name:
                self.curr_storage = storage
                break
