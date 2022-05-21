""" Crawl Update Model """

from datetime import datetime

import pymongo
from db import init_db
from crawls import Crawl, CrawlFile


# =============================================================================
class CrawlUpdater:
    """ Crawl Update """

    def __init__(self):
        _, mdb = init_db()
        self.archives = mdb["archives"]
        self.crawls = mdb["crawls"]
        self.crawl_configs = mdb["crawl_configs"]

    async def inc_crawl_stats(self, crawl: Crawl):
        """ Increment Crawl Stats """

        duration = int((crawl.finished - crawl.started).total_seconds())

        print(f"Duration: {duration}", flush=True)

        # init crawl config stats
        await self.crawl_configs.find_one_and_update(
            {"_id": crawl.cid, "inactive": {"$ne": True}},
            {
                "$inc": {"crawlCount": 1},
                "$set": {
                    "lastCrawlId": crawl.id,
                    "lastCrawlTime": crawl.finished,
                    "lastCrawlState": crawl.state,
                },
            },
        )

        # init archive crawl stats
        yymm = datetime.utcnow().strftime("%Y-%m")
        await self.archives.find_one_and_update(
            {"_id": crawl.aid}, {"$inc": {f"usage.{yymm}": duration}}
        )

    async def store_crawl(self, redis, crawl: Crawl, crawl_file: CrawlFile = None):
        """Add finished crawl to db, increment archive usage.
        If crawl file provided, update and add file"""
        if crawl_file:
            crawl.stats = {
                "done": await redis.llen(f"{crawl.id}:d"),
                "found": await redis.scard(f"{crawl.id}:s"),
            }

            crawl_update = {
                "$set": crawl.to_dict(exclude={"files", "completions"}),
                "$push": {"files": crawl_file.dict()},
            }

            if crawl.state == "complete":
                crawl_update["$inc"] = {"completions": 1}

            await self.crawls.find_one_and_update(
                {"_id": crawl.id},
                crawl_update,
                upsert=True,
            )

        else:
            try:
                await self.crawls.insert_one(crawl.to_dict())
            except pymongo.errors.DuplicateKeyError:
                # print(f"Crawl Already Added: {crawl.id} - {crawl.state}")
                return False

        if crawl.state == "complete":
            await self.inc_crawl_stats(crawl)

        return True
