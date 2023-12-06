import pandas as pd

from exports.googlesearchconsole_last_16m_page_export import GoogleSearchConsoleLast16mPageExport
from exports.screamingfrog_list_crawl_export import ScreamingFrogListCrawlExport
from exports.screamingfrog_sitemap_crawl_export import ScreamingFrogSitemapCrawlExport
from exports.screamingfrog_spider_crawl_export import ScreamingFrogSpiderCrawlExport
from exports.sitebulb_url_internal_export import SitebulbUrlInternalExport
from reports.base_report import BaseReport


class WebsitePerformanceReportRow:
    pass


REPORT_FILENAME = "website_page_report.csv"


class WebsitePagesReport(BaseReport):
    def __init__(self, project):
        super().__init__(project)
        self.report = None
        self.processed_data = None
        self.screamingfrog_list_crawl_export = None
        self.screamingfrog_sitemap_crawl_export = ScreamingFrogSitemapCrawlExport(self.project)
        self.screamingfrog_spider_crawl_export = ScreamingFrogSpiderCrawlExport(self.project)

        self.sitebulb_url_internal_export = SitebulbUrlInternalExport(self.project)
        self.googlesearchconsole_last_16m_page_export = GoogleSearchConsoleLast16mPageExport(self.project)

        self.file_name = REPORT_FILENAME

    def fetch_data(self):
        self.screamingfrog_sitemap_crawl_export.run()
        self.screamingfrog_spider_crawl_export.run()
        self.sitebulb_url_internal_export.run()
        self.googlesearchconsole_last_16m_page_export.run()

    def process_data(self):
        # get unique URLs and clean up
        screamingfrog_sitemap_crawl_data = self.screamingfrog_sitemap_crawl_export.get_data()
        screamingfrog_spider_crawl_data = self.screamingfrog_spider_crawl_export.get_data()
        sitebulb_url_internal_data = self.sitebulb_url_internal_export.get_data()
        googlesearchconsole_last_16m_page_data = self.googlesearchconsole_last_16m_page_export.get_data()

        # Prepare a list for URLs
        urls = []

        # Extract URLs from each DataFrame, if not empty
        if not screamingfrog_sitemap_crawl_data.empty:
            urls.extend(screamingfrog_sitemap_crawl_data['Address'].dropna().tolist())
        if not screamingfrog_spider_crawl_data.empty:
            urls.extend(screamingfrog_spider_crawl_data['Address'].dropna().tolist())
        if not sitebulb_url_internal_data.empty:
            urls.extend(sitebulb_url_internal_data['URL'].dropna().tolist())
        if not googlesearchconsole_last_16m_page_data.empty:
            urls.extend(googlesearchconsole_last_16m_page_data['page'].dropna().tolist())

        # Combine URLs into a single DataFrame with one column
        stacked_data = pd.DataFrame(urls, columns=['full_address'])
        stacked_data.drop_duplicates(inplace=True)

        # clean images
        stacked_data = stacked_data[~stacked_data["full_address"].str.contains(".jpg")]

        self.processed_data = stacked_data

    def generate_report(self):
        self.screamingfrog_list_crawl_export = ScreamingFrogListCrawlExport(self.project, self.processed_data)
        self.screamingfrog_list_crawl_export.run()

        self.report = self.screamingfrog_list_crawl_export.get_data()

        return self.report
