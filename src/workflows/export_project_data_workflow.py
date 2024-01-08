import logging

import pandas as pd

from core.models.project import Project
from core.models.url import UrlManager
from exports.googleanalytics4_last_14m_export import GoogleAnalytics4Last14mExport
from exports.googleanalytics4_last_1m_export import GoogleAnalytics4Last1mExport
from exports.googleanalytics4_last_1m_previous_year_export import GoogleAnalytics4Last1mPreviousYearExport
from exports.googleanalytics4_previous_1m_export import GoogleAnalytics4Previous1mExport
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
from exports.screamingfrog_list_crawl_export import ScreamingFrogListCrawlManualExport, ScreamingFrogListCrawlAutomaticExport
from exports.screamingfrog_sitemap_crawl_export import ScreamingFrogSitemapCrawlAutomaticExport
from exports.screamingfrog_spider_crawl_export import ScreamingFrogSpiderCrawlAutomaticExport
from exports.semrush_analytics_backlinks_rootdomain_export import SemrushAnalyticsBacklinksRootdomainExport
from exports.semrush_analytics_organic_competitors import SemrushAnalyticsOrganicCompetitorsExport
from exports.semrush_analytics_organic_pages_export import SemrushAnalyticsOrganicPagesExport
from exports.semrush_analytics_organic_positions_rootdomain import SemrushAnalyticsOrganicPositionsRootdomainExport
from services.dataframe_service import DataframeService

# Other imports...

logger = logging.getLogger(__name__)


class ExportProjectDataWorkflow:
    def __init__(self, project: Project):
        self.googlesearchconsole_query_previous_15m_export = None
        self.googlesearchconsole_page_query_previous_1m_export = None
        self.googlesearchconsole_page_query_last_1m_previous_year_export = None
        self.googlesearchconsole_page_query_last_1m_export = None
        self.googlesearchconsole_query_previous_1m_export = None
        self.googlesearchconsole_query_last_1m_previous_year_export = None
        self.googlesearchconsole_query_last_1m_export = None
        self.googlesearchconsole_page_previous_1m_export = None
        self.googlesearchconsole_page_last_1m_previous_year_export = None
        self.googlesearchconsole_page_last_1m_export = None
        self.googlesearchconsole_page_query_last_16m_export = None
        self.googlesearchconsole_query_last_16m_export = None
        self.googleanalytics4_previous_1m_export = None
        self.googleanalytics4_last_1m_previous_year_export = None
        self.googleanalytics4_last_1m_export = None
        self.screamingfrog_list_crawl_automatic_export = None
        self.screamingfrog_list_crawl_manual_export = None
        self.googlesearchconsole_page_last_16m_export = None
        self.googleanalytics4_last_14m_export = None
        self.screamingfrog_spider_crawl_automatic_export = None
        self.screamingfrog_sitemap_crawl_automatic_export = None
        self.semrush_analytics_organic_competitors_export = None
        self.semrush_analytics_backlinks_rootdomain_export = None
        self.semrush_analytics_organic_pages_export = None
        self.semrush_analytics_organic_positions_rootdomain_export = None
        self.screamingfrog_spider_crawl_manual_export = None
        self.screamingfrog_sitemap_crawl_manual_export = None
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
        # Run manual exports first
        # self.screamingfrog_sitemap_crawl_manual_export = ScreamingFrogSitemapCrawlManualExport(self._project)
        # self.screamingfrog_sitemap_crawl_manual_export.run(force=True)

        # self.screamingfrog_spider_crawl_manual_export = ScreamingFrogSpiderCrawlManualExport(self._project)
        # self.screamingfrog_spider_crawl_manual_export.run(force=True)

        # self.sitebulb_spider_crawl_url_internal_export = SitebulbSpiderCrawlUrlInternalExport(self._project)
        # self.sitebulb_spider_crawl_url_internal_export.run(force=True)

        self.semrush_analytics_organic_positions_rootdomain_export = SemrushAnalyticsOrganicPositionsRootdomainExport(self._project)
        self.semrush_analytics_organic_positions_rootdomain_export.run()

        self.semrush_analytics_organic_pages_export = SemrushAnalyticsOrganicPagesExport(self._project)
        self.semrush_analytics_organic_pages_export.run()

        self.semrush_analytics_backlinks_rootdomain_export = SemrushAnalyticsBacklinksRootdomainExport(self._project)
        self.semrush_analytics_backlinks_rootdomain_export.run()

        self.semrush_analytics_organic_competitors_export = SemrushAnalyticsOrganicCompetitorsExport(self._project)
        self.semrush_analytics_organic_competitors_export.run()

        self.screamingfrog_sitemap_crawl_automatic_export = ScreamingFrogSitemapCrawlAutomaticExport(self._project)
        self.screamingfrog_sitemap_crawl_automatic_export.run(force=True)

        self.screamingfrog_spider_crawl_automatic_export = ScreamingFrogSpiderCrawlAutomaticExport(self._project)
        self.screamingfrog_spider_crawl_automatic_export.run(force=True)

        # GA4 14 month window
        self.googleanalytics4_last_14m_export = GoogleAnalytics4Last14mExport(self._project)
        self.googleanalytics4_last_14m_export.run(force=True)

        # GSC 16 month window [page]
        self.googlesearchconsole_page_last_16m_export = GoogleSearchConsolePageLast16mExport(self._project)
        self.googlesearchconsole_page_last_16m_export.run(force=True)

    def prepare_stage_2(self):
        screamingfrog_sitemap_crawl_data = self.screamingfrog_sitemap_crawl_automatic_export.get_data()
        screamingfrog_spider_crawl_data = self.screamingfrog_spider_crawl_automatic_export.get_data()

        googleanalytics4_last_14m_data = self.googleanalytics4_last_14m_export.get_data()
        googleanalytics4_last_14m_data["full_address"] = googleanalytics4_last_14m_data["pagePath"].apply(lambda x: UrlManager.build_full_address(self._project.website.root_url, x))

        googlesearchconsole_page_last_16m_data = self.googlesearchconsole_page_last_16m_export.get_data()
        semrush_analytics_organic_positions_rootdomain_data = self.semrush_analytics_organic_positions_rootdomain_export.get_data()
        semrush_analytics_organic_pages_data = self.semrush_analytics_organic_pages_export.get_data()
        semrush_analytics_backlinks_rootdomain_data = self.semrush_analytics_backlinks_rootdomain_export.get_data()

        df = pd.concat(
            [
                DataframeService.get_unique_column_values(screamingfrog_sitemap_crawl_data, column_name="Address"),
                DataframeService.get_unique_column_values(screamingfrog_spider_crawl_data, column_name="Address"),
                DataframeService.get_unique_column_values(googleanalytics4_last_14m_data, column_name="full_address"),
                DataframeService.get_unique_column_values(googlesearchconsole_page_last_16m_data, column_name="page"),
                DataframeService.get_unique_column_values(semrush_analytics_organic_positions_rootdomain_data, column_name="URL"),
                DataframeService.get_unique_column_values(semrush_analytics_organic_pages_data, column_name="URL"),
                DataframeService.get_unique_column_values(semrush_analytics_backlinks_rootdomain_data, column_name="Target url"),
                # DataframeService.get_unique_column_values(self.sitebulb_list_crawl_url_internal_data, column_name="URL"),
            ]
        )

        df = pd.DataFrame(df.unique(), columns=["full_address"])

        # convert to dataframe and cleanup
        # self._project_urls = pd.DataFrame(self._project_urls, columns=["URL"])
        # self._project_urls["URL"] = self._project_urls["URL"].apply(UrlManager.remove_url_fragment)
        self._project_urls = df
        self._project_urls.drop_duplicates(inplace=True)

    def run_stage_2_exports(self):
        # Run manual exports first
        # self.screamingfrog_list_crawl_manual_export = ScreamingFrogListCrawlManualExport(self._project, self._project_urls)
        # self.screamingfrog_list_crawl_manual_export.run(force=True)

        # self.sitebulb_list_crawl_url_internal_export = SitebulbListCrawlUrlInternalExport(self._project, self._project_urls)
        # self.sitebulb_list_crawl_url_internal_export.run(force=True)

        self.screamingfrog_list_crawl_automatic_export = ScreamingFrogListCrawlAutomaticExport(self._project, self._project_urls)
        self.screamingfrog_list_crawl_automatic_export.run(force=True)

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

        # Big fat export that is probably not needed.
        # self.googlesearchconsole_date_page_query_last_16m_export = GoogleSearchConsoleDatePageQueryLast16mExport(self._project)
        # self.googlesearchconsole_date_page_query_last_16m_export.run(force=True)
