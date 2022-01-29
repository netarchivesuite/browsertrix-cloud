type SeedConfig = {
  scopeType?: string;
  limit?: number;
  extraHops?: number;
};

export type CrawlConfig = {
  seeds: (string | ({ url: string } & SeedConfig))[];
} & SeedConfig;

export type CrawlTemplate = {
  id: string;
  name: string;
  schedule: string;
  user: string;
  crawlCount: number;
  lastCrawlId: string;
  lastCrawlTime: string;
  currCrawlId: string;
  config: CrawlConfig;
};