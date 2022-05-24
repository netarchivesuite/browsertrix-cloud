""" K8s support"""

import os
import datetime
import json
import base64
import asyncio

import yaml
import aiohttp

from kubernetes_asyncio import client, config, watch
from kubernetes_asyncio.stream import WsApiClient
from kubernetes_asyncio.client.api_client import ApiClient

from fastapi.templating import Jinja2Templates

from utils import create_from_yaml
from archives import S3Storage


# ============================================================================
CRAWLER_NAMESPACE = os.environ.get("CRAWLER_NAMESPACE") or "crawlers"

# an 2/31 schedule that will never run as empty is not allowed
DEFAULT_NO_SCHEDULE = "* * 31 2 *"


# pylint: disable=too-many-public-methods
# ============================================================================
class K8SManager:
    # pylint: disable=too-many-instance-attributes,too-many-locals,too-many-arguments
    """K8SManager, manager creation of k8s resources from crawl api requests"""

    def __init__(self, namespace=CRAWLER_NAMESPACE):
        config.load_incluster_config()

        self.crawl_ops = None

        self.api_client = ApiClient()

        self.core_api = client.CoreV1Api(self.api_client)
        self.core_api_ws = client.CoreV1Api(api_client=WsApiClient())
        self.batch_api = client.BatchV1Api(self.api_client)
        self.batch_beta_api = client.BatchV1beta1Api(self.api_client)

        self.namespace = namespace
        self._default_storages = {}

        self.crawler_image = os.environ["CRAWLER_IMAGE"]
        self.crawler_image_pull_policy = os.environ["CRAWLER_PULL_POLICY"]

        self.crawl_retries = int(os.environ.get("CRAWLER_RETRIES", "3"))

        self.crawler_liveness_port = int(os.environ.get("CRAWLER_LIVENESS_PORT", 0))

        self.no_delete_jobs = os.environ.get("NO_DELETE_JOBS", "0") != "0"

        self.grace_period = int(os.environ.get("GRACE_PERIOD_SECS", "600"))

        self.requests_cpu = os.environ["CRAWLER_REQUESTS_CPU"]
        self.limits_cpu = os.environ["CRAWLER_LIMITS_CPU"]
        self.requests_mem = os.environ["CRAWLER_REQUESTS_MEM"]
        self.limits_mem = os.environ["CRAWLER_LIMITS_MEM"]

        self.crawl_volume = {"name": "crawl-data"}
        # if set, use persist volume claim for crawls
        crawl_pv_claim = os.environ.get("CRAWLER_PV_CLAIM")
        if crawl_pv_claim:
            self.crawl_volume["persistentVolumeClaim"] = {"claimName": crawl_pv_claim}
        else:
            self.crawl_volume["emptyDir"] = {}

        crawl_node_type = os.environ.get("CRAWLER_NODE_TYPE")
        if crawl_node_type:
            self.crawl_node_selector = {"nodeType": crawl_node_type}
        else:
            self.crawl_node_selector = {}

        self.templates = Jinja2Templates(directory="templates")

        self.loop = asyncio.get_running_loop()
        self.loop.create_task(self.run_event_loop())

    def set_crawl_ops(self, ops):
        """ Set crawl ops handler """
        self.crawl_ops = ops

    async def run_event_loop(self):
        """ Run the job watch loop, retry in case of failure"""
        while True:
            try:
                await self.watch_events()
            # pylint: disable=broad-except
            except Exception as exc:
                print(f"Retrying job loop: {exc}")
                await asyncio.sleep(10)

    async def watch_events(self):
        """ Get events for completed jobs"""
        async with watch.Watch().stream(
            self.core_api.list_namespaced_event,
            self.namespace,
            field_selector="involvedObject.kind=Job",
        ) as stream:
            async for event in stream:
                try:
                    obj = event["object"]
                    if obj.reason == "Completed":
                        self.loop.create_task(
                            self.handle_completed_job(obj.involved_object.name)
                        )

                # pylint: disable=broad-except
                except Exception as exc:
                    print(exc)

    # pylint: disable=unused-argument
    async def check_storage(self, storage_name, is_default=False):
        """Check if storage is valid by trying to get the storage secret
        Will throw if not valid, otherwise return True"""
        await self._get_storage_secret(storage_name)
        return True

    async def update_archive_storage(self, aid, userid, storage):
        """Update storage by either creating a per-archive secret, if using custom storage
        or deleting per-archive secret, if using default storage"""
        archive_storage_name = f"storage-{aid}"
        if storage.type == "default":
            try:
                await self.core_api.delete_namespaced_secret(
                    archive_storage_name,
                    namespace=self.namespace,
                    propagation_policy="Foreground",
                )
            # pylint: disable=bare-except
            except:
                pass

            return

        labels = {"btrix.archive": aid, "btrix.user": userid}

        crawl_secret = client.V1Secret(
            metadata={
                "name": archive_storage_name,
                "namespace": self.namespace,
                "labels": labels,
            },
            string_data={
                "STORE_ENDPOINT_URL": storage.endpoint_url,
                "STORE_ACCESS_KEY": storage.access_key,
                "STORE_SECRET_KEY": storage.secret_key,
            },
        )

        try:
            await self.core_api.create_namespaced_secret(
                namespace=self.namespace, body=crawl_secret
            )

        # pylint: disable=bare-except
        except:
            await self.core_api.patch_namespaced_secret(
                name=archive_storage_name, namespace=self.namespace, body=crawl_secret
            )

    async def add_crawl_config(
        self,
        crawlconfig,
        storage,
        run_now,
        out_filename,
        profile_filename,
    ):
        """add new crawl as cron job, store crawl config in configmap"""

        if storage.type == "default":
            storage_name = storage.name
            storage_path = storage.path
        else:
            storage_name = str(crawlconfig.aid)
            storage_path = ""

        await self.check_storage(storage_name)

        # Create Config Map
        await self._create_config_map(
            crawlconfig,
            STORE_PATH=storage_path,
            STORE_FILENAME=out_filename,
            STORE_NAME=storage_name,
            USER_ID=str(crawlconfig.userid),
            ARCHIVE_ID=str(crawlconfig.aid),
            CRAWL_CONFIG_ID=str(crawlconfig.id),
            INITIAL_SCALE=str(crawlconfig.scale),
        )

        crawl_id = None

        if run_now:
            crawl_id = await self._create_manual_job(crawlconfig)

        return crawl_id

    async def _create_manual_job(self, crawlconfig):
        cid = str(crawlconfig.id)
        ts_now = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
        crawl_id = f"manual-{ts_now}-{cid[:12]}"

        params = {
            "cid": cid,
            "userid": str(crawlconfig.userid),
            "aid": str(crawlconfig.aid),
            "job_image": os.environ.get("JOB_IMAGE"),
            "job_name": "job-" + crawl_id,
            "manual": "1",
        }

        data = self.templates.env.get_template("job.yaml").render(params)

        # create job directly
        await create_from_yaml(self.api_client, data, namespace=self.namespace)

        return crawl_id

    async def _create_scheduled_job(self, crawlconfig):
        """ create or remove cron job based on crawlconfig schedule """
        cid = str(crawlconfig.id)

        cron_jobs = await self.batch_beta_api.list_namespaced_cron_job(
            namespace=self.namespace, label_selector=f"btrix.crawlconfig={cid}"
        )

        cron_job = None

        if len(cron_jobs.items) == 1:
            cron_job = cron_jobs.items[0]

        if cron_job:
            if crawlconfig.schedule and crawlconfig.schedule != cron_job.spec.schedule:
                cron_job.spec.schedule = crawlconfig.schedule
                await self.batch_beta_api.patch_namespaced_cron_job(
                    name=cron_job.metadata.name, namespace=self.namespace, body=cron_job
                )

            if not crawlconfig.schedule:
                await self.batch_beta_api.delete_namespaced_cron_job(
                    name=cron_job.metadata.name, namespace=self.namespace
                )

            return

        if not crawlconfig.schedule:
            return

        # create new cronjob
        params = {
            "cid": cid,
            "userid": str(crawlconfig.userid),
            "aid": str(crawlconfig.aid),
            "job_image": os.environ.get("JOB_IMAGE"),
            "job_name": f"job-scheduled-{cid}",
            "manual": "0",
        }

        data = self.templates.env.get_template("job.yaml").render(params)

        job_yaml = yaml.safe_load(data)
        job_template = job_yaml["spec"]
        metadata = job_yaml["metadata"]

        spec = client.V1beta1CronJobSpec(
            schedule=crawlconfig.schedule,
            suspend=False,
            concurrency_policy="Forbid",
            successful_jobs_history_limit=2,
            failed_jobs_history_limit=3,
            job_template=job_template,
        )

        cron_job = client.V1beta1CronJob(metadata=metadata, spec=spec)

        await self.batch_beta_api.create_namespaced_cron_job(
            namespace=self.namespace, body=cron_job
        )

    async def update_crawl_schedule_or_scale(self, cid, schedule=None, scale=None):
        """ Update the schedule or scale for existing crawl config """

        cron_jobs = await self.batch_beta_api.list_namespaced_cron_job(
            namespace=self.namespace, label_selector=f"btrix.crawlconfig={cid}"
        )

        if len(cron_jobs.items) != 1:
            return

        cron_job = cron_jobs.items[0]

        updated = False

        if schedule is not None:
            real_schedule = schedule or DEFAULT_NO_SCHEDULE

            if real_schedule != cron_job.spec.schedule:
                cron_job.spec.schedule = real_schedule
                cron_job.spec.suspend = not schedule

                cron_job.spec.job_template.metadata.annotations[
                    "btrix.run.schedule"
                ] = schedule
            updated = True

        if scale is not None:
            cron_job.spec.job_template.spec.parallelism = scale
            updated = True

        if updated:
            await self.batch_beta_api.patch_namespaced_cron_job(
                name=cron_job.metadata.name, namespace=self.namespace, body=cron_job
            )

    async def run_crawl_config(self, crawlconfig, userid=None):
        """Run crawl job for cron job based on specified crawlconfig
        optionally set different user"""

        return await self._create_manual_job(crawlconfig)

    async def get_default_storage_access_endpoint(self, name):
        """ Get access_endpoint for default storage """
        return (await self.get_default_storage(name)).access_endpoint_url

    async def get_default_storage(self, name):
        """ get default storage """
        if name not in self._default_storages:
            storage_secret = await self._get_storage_secret(name)

            access_endpoint_url = self._secret_data(
                storage_secret, "STORE_ACCESS_ENDPOINT_URL"
            )
            endpoint_url = self._secret_data(storage_secret, "STORE_ENDPOINT_URL")
            access_key = self._secret_data(storage_secret, "STORE_ACCESS_KEY")
            secret_key = self._secret_data(storage_secret, "STORE_SECRET_KEY")
            region = self._secret_data(storage_secret, "STORE_REGION") or ""

            self._default_storages[name] = S3Storage(
                access_key=access_key,
                secret_key=secret_key,
                endpoint_url=endpoint_url,
                access_endpoint_url=access_endpoint_url,
                region=region,
            )

        return self._default_storages[name]

    # pylint: disable=no-self-use
    def _secret_data(self, secret, name):
        """ decode secret data """
        return base64.standard_b64decode(secret.data[name]).decode()

    async def stop_crawl(self, crawl_id, aid, graceful=True):
        """Attempt to stop crawl, either gracefully by issuing a SIGTERM which
        will attempt to finish current pages

        OR, abruptly by first issueing a SIGABRT, followed by SIGTERM, which
        will terminate immediately"""
        return await self._post_to_job_pods(
            crawl_id, aid, "/cancel" if not graceful else "/stop", {}
        )

    async def scale_crawl(self, crawl_id, aid, scale=1):
        """ Set the crawl scale (job parallelism) on the specified job """

        return await self._post_to_job_pods(crawl_id, aid, "/scale", {"scale": scale})

    async def delete_crawl_configs_for_archive(self, archive):
        """Delete all crawl configs for given archive"""
        return await self._delete_crawl_configs(f"btrix.archive={archive}")

    async def delete_crawl_config_by_id(self, cid):
        """Delete all crawl configs by id"""
        return await self._delete_crawl_configs(f"btrix.crawlconfig={cid}")

    async def handle_completed_job(self, job_name):
        """ Handle completed job: delete """
        # until ttl controller is ready
        if self.no_delete_jobs:
            return

        try:
            await self._delete_job(job_name)
        except:
            pass

    async def run_profile_browser(
        self, userid, aid, command, storage=None, storage_name=None, baseprofile=None
    ):
        """run browser for profile creation """

        # if default storage, use name and path + profiles/
        if storage:
            storage_name = storage.name
            storage_path = storage.path + "profiles/"
        # otherwise, use storage name and existing path from secret
        else:
            storage_path = ""

        # Configure Annotations + Labels
        labels = {
            "btrix.user": userid,
            "btrix.archive": aid,
            "btrix.profile": "1",
        }

        if baseprofile:
            labels["btrix.baseprofile"] = str(baseprofile)

        await self.check_storage(storage_name)

        object_meta = client.V1ObjectMeta(
            generate_name="profile-browser-",
            labels=labels,
        )

        spec = self._get_profile_browser_template(
            command, labels, storage_name, storage_path
        )

        job = client.V1Job(
            kind="Job", api_version="batch/v1", metadata=object_meta, spec=spec
        )

        new_job = await self.batch_api.create_namespaced_job(
            body=job, namespace=self.namespace
        )
        return new_job.metadata.name

    async def get_profile_browser_data(self, name):
        """ return ip and labels for profile browser """
        try:
            job = await self.batch_api.read_namespaced_job(
                name=name, namespace=self.namespace
            )
        # pylint: disable=bare-except
        except:
            # job not found
            return None

        data = job.metadata.labels
        if not data.get("btrix.profile"):
            return None

        pods = await self.core_api.list_namespaced_pod(
            namespace=self.namespace, label_selector=f"job-name={name}"
        )

        for pod in pods.items:
            if pod.status.pod_ip and pod.metadata.labels.get("btrix.profile"):
                data["browser_ip"] = pod.status.pod_ip
                break

        return data

    async def delete_profile_browser(self, browserid):
        """ delete browser job, if it is a profile browser job """
        return await self.handle_completed_job(browserid)

    # ========================================================================
    # Internal Methods

    async def _delete_job(self, name):
        await self.batch_api.delete_namespaced_job(
            name=name,
            namespace=self.namespace,
            grace_period_seconds=60,
            propagation_policy="Foreground",
        )

    async def _create_config_map(self, crawlconfig, **kwargs):
        """ Create Config Map based on CrawlConfig """
        data = kwargs
        data["crawl-config.json"] = json.dumps(crawlconfig.get_raw_config())

        config_map = client.V1ConfigMap(
            metadata={
                "name": f"crawl-config-{crawlconfig.id}",
                "namespace": self.namespace,
                # "labels": labels,
            },
            data=data,
        )

        return await self.core_api.create_namespaced_config_map(
            namespace=self.namespace, body=config_map
        )

    # pylint: disable=unused-argument
    async def _get_storage_secret(self, storage_name):
        """ Check if storage_name is valid by checking existing secret """
        try:
            return await self.core_api.read_namespaced_secret(
                f"storage-{storage_name}",
                namespace=self.namespace,
            )
        except Exception:
            # pylint: disable=broad-except,raise-missing-from
            raise Exception(f"Storage {storage_name} not found")

        return None

    async def _delete_crawl_configs(self, label):
        """Delete Crawl Cron Job and all dependent resources, including configmap and secrets"""

        await self.batch_beta_api.delete_collection_namespaced_cron_job(
            namespace=self.namespace,
            label_selector=label,
            propagation_policy="Foreground",
        )

        await self.core_api.delete_collection_namespaced_config_map(
            namespace=self.namespace,
            label_selector=label,
            propagation_policy="Foreground",
        )

    async def _post_to_job_pods(self, crawl_id, aid, path, data=None):
        job_name = f"job-{crawl_id}"

        pods = await self.core_api.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=f"job-name={job_name},btrix.archive={aid}",
        )

        for pod in pods.items:
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    "POST", f"http://{pod.status.pod_ip}:8000{path}", json=data
                ) as resp:
                    await resp.json()

    def _get_profile_browser_template(
        self, command, labels, storage_name, storage_path
    ):
        return {
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "containers": [
                        {
                            "name": "crawler",
                            "image": self.crawler_image,
                            "imagePullPolicy": self.crawler_image_pull_policy,
                            "command": command,
                            "envFrom": [
                                {"configMapRef": {"name": "shared-crawler-config"}},
                                {"secretRef": {"name": f"storage-{storage_name}"}},
                            ],
                            "env": [
                                {
                                    "name": "CRAWL_ID",
                                    "valueFrom": {
                                        "fieldRef": {
                                            "fieldPath": "metadata.labels['job-name']"
                                        }
                                    },
                                },
                                {"name": "STORE_PATH", "value": storage_path},
                            ],
                        }
                    ],
                    "restartPolicy": "Never",
                    "terminationGracePeriodSeconds": self.grace_period,
                },
            }
        }
