""" entry point for K8S browser job (eg. for profile creation) """

from .base_job import SwarmBaseJob
from ..profile_job import BrowserJob


# =============================================================================
class SwarmBrowserJob(SwarmBaseJob, BrowserJob):
#class SwarmBrowserJob(BrowserJob, SwarmBaseJob):
    """ Browser run job """


if __name__ == "__main__":
    job = SwarmBrowserJob()
    job.loop.run_forever()
