""" entry point for K8S browser job (eg. for profile creation) """

from .base_job import SwarmBaseJob
from ..profile_job import ProfileJob


# =============================================================================
class SwarmProfileJob(SwarmBaseJob, ProfileJob):
    # class SwarmProfileJob(ProfileJob, SwarmBaseJob):
    """ Browser run job """


if __name__ == "__main__":
    job = SwarmProfileJob()
    job.loop.run_forever()
