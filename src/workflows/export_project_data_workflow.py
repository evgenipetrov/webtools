import logging

import pandas as pd

from core.models import Project
from core.models.url import UrlManager
from exports.googleanalytics4_last_14m_export import GoogleAnalytics4Last14mExport
from exports.googleanalytics4_last_1m_export import GoogleAnalytics4Last1mExport
from exports.googleanalytics4_last_1m_previous_year_export import GoogleAnalytics4Last1mPreviousYearExport
from exports.googleanalytics4_previous_1m_export import GoogleAnalytics4Previous1mExport
from exports.googlesearchconsole_date_page_query_last_16m_export import GoogleSearchConsoleDatePageQueryLast16mExport
from exports.googlesearchconsole_page_last_16m_export import GoogleSearchConsolePageLast16mExport
from exports.googlesearchconsole_page_last_1m_export import GoogleSearchConsolePageLast1mExport
from exports.googlesearchconsole_page_last_1m_previous_year_export import GoogleSearchConsolePageLast1mPreviousYearExport
from exports.googlesearchconsole_page_previous_1m_export import GoogleSearchConsolePagePrevious1mExport
from exports.googlesearchconsole_page_query_last_16m_export import GoogleSearchConsolePageQueryLast16mExport
from exports.googlesearchconsole_page_query_last_1m_export import GoogleSearchConsolePageQueryLast1mExport
from exports.googlesearchconsole_page_query_last_1m_previous_year_export import GoogleSearchConsolePageQueryLast1mPreviousYearExport
from exports.googlesearchconsole_page_query_previous_1m_export import GoogleSearchConsolePageQueryPrevious1mExport
from exports.googlesearchconsole_query_last_16m_export import GoogleSearchConsoleQueryLast16mExport
from exports.googlesearchconsole_query_last_1m_export import GoogleSearchConsoleQueryLast1mExport
from exports.googlesearchconsole_query_last_1m_previous_year_export import GoogleSearchConsoleQueryLast1mPreviousYearExport
from exports.googlesearchconsole_query_previous_15m_export import GoogleSearchConsoleQueryPrevious15mExport
from exports.googlesearchconsole_query_previous_1m_export import GoogleSearchConsoleQueryPrevious1mExport
from exports.screamingfrog_list_crawl_export import ScreamingFrogListCrawlExport
from exports.screamingfrog_sitemap_crawl_export import ScreamingFrogSitemapCrawlExport
from exports.screamingfrog_spider_crawl_export import ScreamingFrogSpiderCrawlExport
from exports.semrush_analytics_backlinks_rootdomain_export import SemrushAnalyticsBacklinksRootdomainExport
from exports.semrush_analytics_organic_competitors import SemrushAnalyticsOrganicCompetitorsExport
from exports.semrush_analytics_organic_pages_export import SemrushAnalyticsOrganicPagesExport
from exports.semrush_analytics_organic_positions_rootdomain import SemrushAnalyticsOrganicPositionsRootdomainExport
from exports.sitebulb_list_crawl_url_internal_export import SitebulbListCrawlUrlInternalExport
from exports.sitebulb_spider_crawl_url_internal_export import SitebulbSpiderCrawlUrlInternalExport

# Other imports...

logger = logging.getLogger(__name__)


class ExportProjectDataWorkflow:
    # googleanalytics4
    googleanalytics4_last_14m_export: GoogleAnalytics4Last14mExport
    googleanalytics4_last_1m_export: GoogleAnalytics4Last1mExport
    googleanalytics4_last_1m_previous_year_export: GoogleAnalytics4Last1mPreviousYearExport
    googleanalytics4_previous_1m_export: GoogleAnalytics4Previous1mExport
    # googlesearchconsole
    googlesearchconsole_date_page_query_last_16m_export: GoogleSearchConsoleDatePageQueryLast16mExport
    googlesearchconsole_page_last_16m_export: GoogleSearchConsolePageLast16mExport
    googlesearchconsole_page_last_1m_export: GoogleSearchConsolePageLast1mExport
    googlesearchconsole_page_last_1m_previous_year_export: GoogleSearchConsolePageLast1mPreviousYearExport
    googlesearchconsole_page_previous_1m_export: GoogleSearchConsolePagePrevious1mExport
    googlesearchconsole_page_query_last_16m_export: GoogleSearchConsolePageQueryLast16mExport
    googlesearchconsole_page_query_last_1m_export: GoogleSearchConsolePageQueryLast1mExport
    googlesearchconsole_page_query_last_1m_previous_year_export: GoogleSearchConsolePageQueryLast1mPreviousYearExport
    googlesearchconsole_page_query_previous_1m_export: GoogleSearchConsolePageQueryPrevious1mExport
    googlesearchconsole_query_last_16m_export: GoogleSearchConsoleQueryLast16mExport
    googlesearchconsole_query_last_1m_export: GoogleSearchConsoleQueryLast1mExport
    googlesearchconsole_query_last_1m_previous_year_export: GoogleSearchConsoleQueryLast1mPreviousYearExport
    googlesearchconsole_query_previous_15m_export: GoogleSearchConsoleQueryPrevious15mExport
    googlesearchconsole_query_previous_1m_export: GoogleSearchConsoleQueryPrevious1mExport
    # screamingfrog
    screamingfrog_list_crawl_export: ScreamingFrogListCrawlExport
    screamingfrog_sitemap_crawl_export: ScreamingFrogSitemapCrawlExport
    screamingfrog_spider_crawl_export: ScreamingFrogSpiderCrawlExport
    # semrush
    semrush_analytics_backlinks_rootdomain_export: SemrushAnalyticsBacklinksRootdomainExport
    semrush_analytics_organic_competitors_export: SemrushAnalyticsOrganicCompetitorsExport
    semrush_analytics_organic_pages_export: SemrushAnalyticsOrganicPagesExport
    semrush_analytics_organic_positions_rootdomain_export: SemrushAnalyticsOrganicPositionsRootdomainExport
    # sitebulb
    sitebulb_list_crawl_url_internal_export: SitebulbListCrawlUrlInternalExport
    sitebulb_spider_crawl_url_internal_export: SitebulbSpiderCrawlUrlInternalExport

    def __init__(self, project: Project):
        self._project = project
        self._project_urls = []

    def run(self):
        self.prepare_stage_1()
        self.run_stage_1_exports()
        self.prepare_stage_2()
        self.run_stage_2_exports()
        logger.info(f"Export project data workflow for '{self._project.name}' completed.")

    def prepare_stage_1(self):
        pass

    def run_stage_1_exports(self):
        # GA4 14 month window
        self.googleanalytics4_last_14m_export = GoogleAnalytics4Last14mExport(self._project)
        self.googleanalytics4_last_14m_export.run(force=True)

        # GSC 16 month window [page]
        self.googlesearchconsole_page_last_16m_export = GoogleSearchConsolePageLast16mExport(self._project)
        self.googlesearchconsole_page_last_16m_export.run(force=True)

        self.screamingfrog_sitemap_crawl_export = ScreamingFrogSitemapCrawlExport(self._project)
        self.screamingfrog_sitemap_crawl_export.run(force=True)

        self.screamingfrog_spider_crawl_export = ScreamingFrogSpiderCrawlExport(self._project)
        self.screamingfrog_spider_crawl_export.run(force=True)

        self.sitebulb_spider_crawl_url_internal_export = SitebulbSpiderCrawlUrlInternalExport(self._project)
        self.sitebulb_spider_crawl_url_internal_export.run(force=True)

        self.semrush_analytics_organic_positions_rootdomain_export = SemrushAnalyticsOrganicPositionsRootdomainExport(self._project)
        self.semrush_analytics_organic_positions_rootdomain_export.run(force=True)

        self.semrush_analytics_organic_pages_export = SemrushAnalyticsOrganicPagesExport(self._project)
        self.semrush_analytics_organic_pages_export.run(force=True)

        self.semrush_analytics_backlinks_rootdomain_export = SemrushAnalyticsBacklinksRootdomainExport(self._project)
        self.semrush_analytics_backlinks_rootdomain_export.run(force=True)

    def prepare_stage_2(self):
        if self.screamingfrog_sitemap_crawl_export:
            screamingfrog_sitemap_crawl_data = self.screamingfrog_sitemap_crawl_export.get_data()
            self._project_urls.extend(screamingfrog_sitemap_crawl_data["Address"].dropna().unique().tolist())

        if self.screamingfrog_spider_crawl_export:
            screamingfrog_spider_crawl_data = self.screamingfrog_spider_crawl_export.get_data()
            self._project_urls.extend(screamingfrog_spider_crawl_data["Address"].dropna().unique().tolist())

        if self.sitebulb_spider_crawl_url_internal_export:
            sitebulb_data = self.sitebulb_spider_crawl_url_internal_export.get_data()
            self._project_urls.extend(sitebulb_data["URL"].dropna().unique().tolist())

        if self.semrush_analytics_organic_positions_rootdomain_export:
            semrush_positions_data = self.semrush_analytics_organic_positions_rootdomain_export.get_data()
            self._project_urls.extend(semrush_positions_data["URL"].dropna().unique().tolist())

        if self.semrush_analytics_organic_pages_export:
            semrush_pages_data = self.semrush_analytics_organic_pages_export.get_data()
            self._project_urls.extend(semrush_pages_data["URL"].dropna().unique().tolist())

        if self.semrush_analytics_backlinks_rootdomain_export:
            semrush_backlinks_data = self.semrush_analytics_backlinks_rootdomain_export.get_data()
            self._project_urls.extend(semrush_backlinks_data["Target url"].dropna().unique().tolist())

        if self.googleanalytics4_last_14m_export:
            ga4_data = self.googleanalytics4_last_14m_export.get_data()
            ga4_data["full_address"] = ga4_data["pagePath"].apply(lambda path: UrlManager.build_full_address(self._project.base_url, path))
            self._project_urls.extend(ga4_data["full_address"].dropna().unique().tolist())

        if self.googlesearchconsole_page_last_16m_export:
            gsc_data = self.googlesearchconsole_page_last_16m_export.get_data()
            self._project_urls.extend(gsc_data["page"].dropna().unique().tolist())

        # convert to dataframe and cleanup
        self._project_urls = pd.DataFrame(self._project_urls, columns=["URL"])
        self._project_urls["URL"] = self._project_urls["URL"].apply(UrlManager.remove_url_fragment)
        self._project_urls.drop_duplicates(inplace=True)

    def run_stage_2_exports(self):
        self.screamingfrog_list_crawl_export = ScreamingFrogListCrawlExport(self._project, self._project_urls)
        self.screamingfrog_list_crawl_export.run(force=True)

        self.sitebulb_list_crawl_url_internal_export = SitebulbListCrawlUrlInternalExport(self._project, self._project_urls)
        self.sitebulb_list_crawl_url_internal_export.run(force=True)

        self.semrush_analytics_organic_competitors_export = SemrushAnalyticsOrganicCompetitorsExport(self._project)
        self.semrush_analytics_organic_competitors_export.run(force=True)

        # GA4 1 month window
        self.googleanalytics4_last_1m_export = GoogleAnalytics4Last1mExport(self._project)
        self.googleanalytics4_last_1m_export.run(force=True)

        self.googleanalytics4_last_1m_previous_year_export = GoogleAnalytics4Last1mPreviousYearExport(self._project)
        self.googleanalytics4_last_1m_previous_year_export.run(force=True)

        self.googleanalytics4_previous_1m_export = GoogleAnalytics4Previous1mExport(self._project)
        self.googleanalytics4_previous_1m_export.run(force=True)

        # GSC 16 month window [query]
        self.googlesearchconsole_query_last_16m_export = GoogleSearchConsoleQueryLast16mExport(self._project)
        self.googlesearchconsole_query_last_16m_export.run(force=True)

        # GSC 16 month window [page, query]
        self.googlesearchconsole_page_query_last_16m_export = GoogleSearchConsolePageQueryLast16mExport(self._project)
        self.googlesearchconsole_page_query_last_16m_export.run(force=True)

        # GSC 1 month window [page]
        self.googlesearchconsole_page_last_1m_export = GoogleSearchConsolePageLast1mExport(self._project)
        self.googlesearchconsole_page_last_1m_export.run(force=True)

        self.googlesearchconsole_page_last_1m_previous_year_export = GoogleSearchConsolePageLast1mPreviousYearExport(self._project)
        self.googlesearchconsole_page_last_1m_previous_year_export.run(force=True)

        self.googlesearchconsole_page_previous_1m_export = GoogleSearchConsolePagePrevious1mExport(self._project)
        self.googlesearchconsole_page_previous_1m_export.run(force=True)

        # GSC 1 month window [query]
        self.googlesearchconsole_query_last_1m_export = GoogleSearchConsoleQueryLast1mExport(self._project)
        self.googlesearchconsole_query_last_1m_export.run(force=True)

        self.googlesearchconsole_query_last_1m_previous_year_export = GoogleSearchConsoleQueryLast1mPreviousYearExport(self._project)
        self.googlesearchconsole_query_last_1m_previous_year_export.run(force=True)

        self.googlesearchconsole_query_previous_1m_export = GoogleSearchConsoleQueryPrevious1mExport(self._project)
        self.googlesearchconsole_query_previous_1m_export.run(force=True)

        # GSC 1 month window [page, query]
        self.googlesearchconsole_page_query_last_1m_export = GoogleSearchConsolePageQueryLast1mExport(self._project)
        self.googlesearchconsole_page_query_last_1m_export.run(force=True)

        self.googlesearchconsole_page_query_last_1m_previous_year_export = GoogleSearchConsolePageQueryLast1mPreviousYearExport(self._project)
        self.googlesearchconsole_page_query_last_1m_previous_year_export.run(force=True)

        self.googlesearchconsole_page_query_previous_1m_export = GoogleSearchConsolePageQueryPrevious1mExport(self._project)
        self.googlesearchconsole_page_query_previous_1m_export.run(force=True)

        # GSC special exports
        self.googlesearchconsole_query_previous_15m_export = GoogleSearchConsoleQueryPrevious15mExport(self._project)
        self.googlesearchconsole_query_previous_15m_export.run(force=True)

        self.googlesearchconsole_date_page_query_last_16m_export = GoogleSearchConsoleDatePageQueryLast16mExport(self._project)
        self.googlesearchconsole_date_page_query_last_16m_export.run(force=True)
