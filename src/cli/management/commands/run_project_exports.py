import logging
from concurrent.futures import ThreadPoolExecutor, wait
import pandas as pd
from urllib.parse import urlunparse
from django.core.management.base import BaseCommand

from core.managers.url_manager import UrlManager
from core.models.project import ProjectManager
from exports.googleanalytics4_last_14m_export import GoogleAnalytics4ExportLast14m
from exports.googleanalytics4_last_1m_export import GoogleAnalytics4ExportLast1m
from exports.googleanalytics4_last_1m_previous_year_export import GoogleAnalytics4ExportLast1mPreviousYearExport
from exports.googleanalytics4_previous_1m_export import GoogleAnalytics4ExportPrevious1m
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

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Runs all exports."

    def __init__(self, stdout=None, stderr=None, no_color=False, force_color=False):
        super().__init__(stdout, stderr, no_color, force_color)
        self.googlesearchconsole_date_page_query_last_16m_export = None
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
        self.semrush_analytics_backlinks_rootdomain_export = None
        self.semrush_analytics_organic_competitors_export = None
        self.sitebulb_list_crawl_url_internal_export = None
        self.screamingfrog_list_crawl_export = None
        self.googlesearchconsole_page_last_16m_export = None
        self.googleanalytics4_last_14m_export = None
        self.semrush_analytics_organic_pages_export = None
        self.semrush_analytics_organic_positions_rootdomain_export = None
        self.sitebulb_spider_crawl_url_internal_export = None
        self.screamingfrog_spider_crawl_export = None
        self.screamingfrog_sitemap_crawl_export = None
        self.url_list = None

    def add_arguments(self, parser):
        parser.add_argument(
            "--project-name",
            type=str,
            help="Name of the project to run exports for",
            required=True,
        )

    def handle(self, *args, **options):
        project_name = options["project_name"]
        project = ProjectManager.get_project_by_name(project_name)

        if not project:
            project = ProjectManager.create_project_by_name(project_name)

        # Using ThreadPoolExecutor to run exports in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit parallel tasks
            stage_1_manual_exports = executor.submit(self.handle_stage_1_manual_exports, project)
            stage_1_automated_exports = executor.submit(self.handle_stage_1_automated_exports, project)

            # Wait for all tasks to complete
            wait(
                [
                    stage_1_manual_exports,
                    stage_1_automated_exports,
                ]
            )

        self.handle_stage_2_preparation(project)
        # Using ThreadPoolExecutor to run exports in parallel
        with ThreadPoolExecutor(max_workers=1) as executor:
            # Submit parallel tasks
            stage_2_manual_exports = executor.submit(self.handle_stage_2_manual_exports, project)
            stage_2_automated_exports = executor.submit(self.handle_stage_2_automated_exports, project)

            # Wait for all tasks to complete
            wait(
                [
                    stage_2_manual_exports,
                    stage_2_automated_exports,
                ]
            )

        logger.info(f"Exports run for '{project.name}' completed.")

    def handle_stage_1_manual_exports(self, project):
        self.screamingfrog_sitemap_crawl_export = ScreamingFrogSitemapCrawlExport(project)
        self.screamingfrog_sitemap_crawl_export.run(force=True)

        self.screamingfrog_spider_crawl_export = ScreamingFrogSpiderCrawlExport(project)
        self.screamingfrog_spider_crawl_export.run(force=True)

        self.sitebulb_spider_crawl_url_internal_export = SitebulbSpiderCrawlUrlInternalExport(project)
        self.sitebulb_spider_crawl_url_internal_export.run(force=True)

        self.semrush_analytics_organic_positions_rootdomain_export = SemrushAnalyticsOrganicPositionsRootdomainExport(project)
        self.semrush_analytics_organic_positions_rootdomain_export.run(force=True)

        self.semrush_analytics_organic_pages_export = SemrushAnalyticsOrganicPagesExport(project)
        self.semrush_analytics_organic_pages_export.run(force=True)

        self.semrush_analytics_backlinks_rootdomain_export = SemrushAnalyticsBacklinksRootdomainExport(project)
        self.semrush_analytics_backlinks_rootdomain_export.run(force=True)

    def handle_stage_1_automated_exports(self, project):
        # GA4 14 month window
        self.googleanalytics4_last_14m_export = GoogleAnalytics4ExportLast14m(project)
        self.googleanalytics4_last_14m_export.run(force=True)

        # GSC 16 month window [page]
        self.googlesearchconsole_page_last_16m_export = GoogleSearchConsolePageLast16mExport(project)
        self.googlesearchconsole_page_last_16m_export.run(force=True)

    def handle_stage_2_preparation(self, project):
        self.url_list = []

        if self.screamingfrog_sitemap_crawl_export:
            screamingfrog_sitemap_crawl_data = self.screamingfrog_sitemap_crawl_export.get_data()
            self.url_list.extend(screamingfrog_sitemap_crawl_data["Address"].dropna().unique().tolist())

        if self.screamingfrog_spider_crawl_export:
            screamingfrog_spider_crawl_data = self.screamingfrog_spider_crawl_export.get_data()
            self.url_list.extend(screamingfrog_spider_crawl_data["Address"].dropna().unique().tolist())

        if self.sitebulb_spider_crawl_url_internal_export:
            sitebulb_data = self.sitebulb_spider_crawl_url_internal_export.get_data()
            self.url_list.extend(sitebulb_data["URL"].dropna().unique().tolist())

        if self.semrush_analytics_organic_positions_rootdomain_export:
            semrush_positions_data = self.semrush_analytics_organic_positions_rootdomain_export.get_data()
            self.url_list.extend(semrush_positions_data["URL"].dropna().unique().tolist())

        if self.semrush_analytics_organic_pages_export:
            semrush_pages_data = self.semrush_analytics_organic_pages_export.get_data()
            self.url_list.extend(semrush_pages_data["URL"].dropna().unique().tolist())

        if self.semrush_analytics_backlinks_rootdomain_export:
            semrush_backlinks_data = self.semrush_analytics_backlinks_rootdomain_export.get_data()
            self.url_list.extend(semrush_backlinks_data["Target url"].dropna().unique().tolist())

        if self.googleanalytics4_last_14m_export:
            ga4_data = self.googleanalytics4_last_14m_export.get_data()
            ga4_data["full_address"] = ga4_data["pagePath"].apply(lambda path: UrlManager.create_full_address(project.base_url, path))
            self.url_list.extend(ga4_data["full_address"].dropna().unique().tolist())

        if self.googlesearchconsole_page_last_16m_export:
            gsc_data = self.googlesearchconsole_page_last_16m_export.get_data()
            self.url_list.extend(gsc_data["page"].dropna().unique().tolist())

        # convert to dataframe and cleanup
        self.url_list = pd.DataFrame(self.url_list, columns=["URL"])
        self.url_list["URL"] = self.url_list["URL"].apply(UrlManager.remove_url_fragment)
        self.url_list.drop_duplicates(inplace=True)

    def handle_stage_2_manual_exports(self, project):
        self.screamingfrog_list_crawl_export = ScreamingFrogListCrawlExport(project, self.url_list)
        self.screamingfrog_list_crawl_export.run(force=True)

        self.sitebulb_list_crawl_url_internal_export = SitebulbListCrawlUrlInternalExport(project, self.url_list)
        self.sitebulb_list_crawl_url_internal_export.run(force=True)

        self.semrush_analytics_organic_competitors_export = SemrushAnalyticsOrganicCompetitorsExport(project)
        self.semrush_analytics_organic_competitors_export.run(force=True)

    def handle_stage_2_automated_exports(self, project):
        # GA4 1 month window
        self.googleanalytics4_last_1m_export = GoogleAnalytics4ExportLast1m(project)
        self.googleanalytics4_last_1m_export.run(force=True)

        self.googleanalytics4_last_1m_previous_year_export = GoogleAnalytics4ExportLast1mPreviousYearExport(project)
        self.googleanalytics4_last_1m_previous_year_export.run(force=True)

        self.googleanalytics4_previous_1m_export = GoogleAnalytics4ExportPrevious1m(project)
        self.googleanalytics4_previous_1m_export.run(force=True)

        # GSC 16 month window [query]
        self.googlesearchconsole_query_last_16m_export = GoogleSearchConsoleQueryLast16mExport(project)
        self.googlesearchconsole_query_last_16m_export.run(force=True)

        # GSC 16 month window [page, query]
        self.googlesearchconsole_page_query_last_16m_export = GoogleSearchConsolePageQueryLast16mExport(project)
        self.googlesearchconsole_page_query_last_16m_export.run(force=True)

        # GSC 1 month window [page]
        self.googlesearchconsole_page_last_1m_export = GoogleSearchConsolePageLast1mExport(project)
        self.googlesearchconsole_page_last_1m_export.run(force=True)

        self.googlesearchconsole_page_last_1m_previous_year_export = GoogleSearchConsolePageLast1mPreviousYearExport(project)
        self.googlesearchconsole_page_last_1m_previous_year_export.run(force=True)

        self.googlesearchconsole_page_previous_1m_export = GoogleSearchConsolePagePrevious1mExport(project)
        self.googlesearchconsole_page_previous_1m_export.run(force=True)

        # GSC 1 month window [query]
        self.googlesearchconsole_query_last_1m_export = GoogleSearchConsoleQueryLast1mExport(project)
        self.googlesearchconsole_query_last_1m_export.run(force=True)

        self.googlesearchconsole_query_last_1m_previous_year_export = GoogleSearchConsoleQueryLast1mPreviousYearExport(project)
        self.googlesearchconsole_query_last_1m_previous_year_export.run(force=True)

        self.googlesearchconsole_query_previous_1m_export = GoogleSearchConsoleQueryPrevious1mExport(project)
        self.googlesearchconsole_query_previous_1m_export.run(force=True)

        # GSC 1 month window [page, query]
        self.googlesearchconsole_page_query_last_1m_export = GoogleSearchConsolePageQueryLast1mExport(project)
        self.googlesearchconsole_page_query_last_1m_export.run(force=True)

        self.googlesearchconsole_page_query_last_1m_previous_year_export = GoogleSearchConsolePageQueryLast1mPreviousYearExport(project)
        self.googlesearchconsole_page_query_last_1m_previous_year_export.run(force=True)

        self.googlesearchconsole_page_query_previous_1m_export = GoogleSearchConsolePageQueryPrevious1mExport(project)
        self.googlesearchconsole_page_query_previous_1m_export.run(force=True)

        # GSC special exports
        self.googlesearchconsole_query_previous_15m_export = GoogleSearchConsoleQueryPrevious15mExport(project)
        self.googlesearchconsole_query_previous_15m_export.run(force=True)

        self.googlesearchconsole_date_page_query_last_16m_export = GoogleSearchConsoleDatePageQueryLast16mExport(project)
        self.googlesearchconsole_date_page_query_last_16m_export.run(force=True)
