""" Create and Update Running Crawl within Crawl Job """

import os
from datetime import datetime

import pymongo
from db import init_db
from crawls import Crawl, CrawlFile, CrawlCompleteIn, ts_now


# =============================================================================
# pylint: disable=too-many-instance-attributes,bare-except
class CrawlUpdater:
    """ Crawl Update """

    def __init__(self, id_):
        _, mdb = init_db()
        self.archives = mdb["archives"]
        self.crawls = mdb["crawls"]
        self.crawl_configs = mdb["crawl_configs"]

        self.crawl_id = id_

        self.aid = os.environ.get("ARCHIVE_ID")
        self.cid = os.environ.get("CRAWL_CONFIG_ID")
        self.userid = os.environ.get("USER_ID")
        self.is_manual = os.environ.get("RUN_MANUAL") == "1"

        self.storage_path = os.environ.get("STORE_PATH")
        self.storage_name = os.environ.get("STORE_NAME")

        self.last_stats = None

    async def inc_crawl_complete_stats(self, started, finished, state):
        """ Increment Crawl Stats """

        duration = int((finished - started).total_seconds())

        print(f"Duration: {duration}", flush=True)

        # init crawl config stats
        await self.crawl_configs.find_one_and_update(
            {"_id": self.cid, "inactive": False},
            {
                "$inc": {"crawlCount": 1},
                "$set": {
                    "lastCrawlId": self.crawl_id,
                    "lastCrawlTime": finished,
                    "lastCrawlState": state,
                },
            },
        )

        # init archive crawl stats
        yymm = datetime.utcnow().strftime("%Y-%m")
        await self.archives.find_one_and_update(
            {"_id": self.aid}, {"$inc": {f"usage.{yymm}": duration}}
        )

    async def update_running_crawl_stats(self, redis, crawl_id):
        """ update stats for running crawl """
        stats = await self._get_running_stats(redis, crawl_id)
        if self.last_stats == stats:
            return

        if not self.last_stats:
            await self.update_state("running", finished=False)

        await self.crawls.find_one_and_update(
            {"_id": crawl_id},
            {
                "$set": {"stats": stats},
            },
        )

        self.last_stats = stats

    async def update_state(self, state, finished=False, start_time=None):
        """ update crawl state, and optionally mark as finished """
        update = {"state": state}

        if finished:
            update["finished"] = ts_now()

        if start_time:
            update["started"] = start_time

        await self.crawls.find_one_and_update({"_id": self.crawl_id}, {"$set": update})

        return update

    async def init_crawl(self, scale=1):
        """ create crawl, doesn't exist, mark as starting """
        start_time = ts_now()

        try:
            crawl = self._make_crawl("starting", start_time, scale)
            await self.crawls.insert_one(crawl.to_dict())
        except pymongo.errors.DuplicateKeyError:
            await self.update_state("starting", finished=False, start_time=start_time)

        return start_time

    async def add_file_to_crawl(self, cc_data, started):
        """ Handle crawl complete message, add as CrawlFile to db """

        crawlcomplete = CrawlCompleteIn(**cc_data)

        state = "complete" if crawlcomplete.completed else "partial_complete"

        inx = None
        filename = None
        if self.storage_path:
            inx = crawlcomplete.filename.index(self.storage_path)
            filename = (
                crawlcomplete.filename[inx:] if inx > 0 else crawlcomplete.filename
            )
            # storage_name = job.metadata.annotations.get("btrix.storage_name")

        def_storage_name = self.storage_name if inx else None

        crawl_file = CrawlFile(
            def_storage_name=def_storage_name,
            filename=filename or crawlcomplete.filename,
            size=crawlcomplete.size,
            hash=crawlcomplete.hash,
        )

        update = {"state": state}

        if state == "complete":
            update["finished"] = ts_now()

        await self.crawls.find_one_and_update(
            {"_id": self.crawl_id},
            {
                "$set": update,
                "$push": {"files": crawl_file.dict()},
            },
        )

        if state == "complete":
            await self.inc_crawl_complete_stats(started, update["finished"], state)

        return True

    async def _get_running_stats(self, redis, crawl_id):
        """ get stats from redis for running or finished crawl """
        return {
            "done": await redis.llen(f"{crawl_id}:d"),
            "found": await redis.scard(f"{crawl_id}:s"),
        }

    def _make_crawl(self, state, start_time, scale):
        """ Create crawl object for partial or fully complete crawl """
        return Crawl(
            id=self.crawl_id,
            state=state,
            userid=self.userid,
            aid=self.aid,
            cid=self.cid,
            manual=self.is_manual,
            scale=scale,
            started=start_time,
            # colls=json.loads(job.metadata.annotations.get("btrix.colls", [])),
        )
