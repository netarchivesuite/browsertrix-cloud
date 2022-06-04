""" base k8s job driver """

import os
import asyncio

import sys
import yaml

from fastapi.templating import Jinja2Templates

from .utils import get_templates_dir, run_swarm_stack, delete_swarm_stack
from ..utils import random_suffix


# =============================================================================
# pylint: disable=too-many-instance-attributes,bare-except,broad-except
class SwarmJobMixin:
    """ Crawl Job State """

    def __init__(self):
        self.shared_config_file = os.environ.get("SHARED_JOB_CONFIG")
        self.custom_config_file = os.environ.get("CUSTOM_JOB_CONFIG")
        self.shared_secrets_file = os.environ.get("STORAGE_SECRETS")

        self.curr_storage = {}

        self.job_id = os.environ.get("JOB_ID")

        if os.environ.get("RUN_MANUAL") == "0":
            self.job_id += "-" + random_suffix()

        self.prefix = os.environ.get("STACK_PREFIX", "stack-")

        if self.custom_config_file:
            self._populate_env("/" + self.custom_config_file)

        self.templates = Jinja2Templates(directory=get_templates_dir())

        super().__init__()

    # pylint: disable=no-self-use
    def _populate_env(self, filename):
        with open(filename) as fh_config:
            params = yaml.safe_load(fh_config)

        for key in params:
            val = params[key]
            if isinstance(val, str):
                os.environ[key] = val

    async def init_job_objects(self, template, extra_params=None):
        """ init swarm objects from specified template with given extra_params """
        if self.shared_config_file:
            with open("/" + self.shared_config_file) as fh_config:
                params = yaml.safe_load(fh_config)
        else:
            params = {}

        params["id"] = self.job_id

        if extra_params:
            params.update(extra_params)

        if (
            os.environ.get("STORAGE_NAME")
            and self.shared_secrets_file
            and not self.curr_storage
        ):
            self.load_storage(
                f"/var/run/secrets/{self.shared_secrets_file}",
                os.environ.get("STORAGE_NAME"),
            )

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

        if os.environ.get("RUN_MANUAL") != "0":
            print("Removed other objects, removing ourselves", flush=True)
            await loop.run_in_executor(None, delete_swarm_stack, f"job-{self.job_id}")
        else:
            sys.exit(0)

        return True

    def load_storage(self, filename, storage_name):
        """ load storage credentials for given storage from yaml file """
        with open(filename) as fh_config:
            data = yaml.safe_load(fh_config.read())

        if not data or not data.get("storages"):
            return

        for storage in data["storages"]:
            if storage.get("name") == storage_name:
                self.curr_storage = storage
                break
