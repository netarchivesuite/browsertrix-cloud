""" entry point for K8S browser job (eg. for profile creation) """

from .base_job import K8SBaseJob
from ..profile_job import BrowserJob


# =============================================================================
class K8SBrowserJob(K8SBaseJob, BrowserJob):
    """ Browser run job """


if __name__ == "__main__":
    job = K8SBrowserJob()
    job.loop.run_forever()
