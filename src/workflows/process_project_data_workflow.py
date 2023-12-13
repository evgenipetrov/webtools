import logging

import pandas as pd
from django.db import transaction

from core.models import Project
from core.models.url import UrlManager
from core.models.website import WebsiteManager
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

logger = logging.getLogger(__name__)


class ProcessProjectDataWorkflow:
    def __init__(self, project: Project):
        self._project = project
        # googleanalytics4
        self.googleanalytics4_last_14m_data = None
        self.googleanalytics4_last_1m_data = None
        self.googleanalytics4_last_1m_previous_year_data = None
        self.googleanalytics4_previous_1m_data = None
        # googlesearchconsole
        self.googlesearchconsole_date_page_query_last_16m_data = None
        self.googlesearchconsole_page_last_16m_data = None
        self.googlesearchconsole_page_last_1m_data = None
        self.googlesearchconsole_page_last_1m_previous_year_data = None
        self.googlesearchconsole_page_previous_1m_data = None
        self.googlesearchconsole_page_query_last_16m_data = None
        self.googlesearchconsole_page_query_last_1m_data = None
        self.googlesearchconsole_page_query_last_1m_previous_year_data = None
        self.googlesearchconsole_page_query_previous_1m_data = None
        self.googlesearchconsole_query_last_16m_data = None
        self.googlesearchconsole_query_last_1m_data = None
        self.googlesearchconsole_query_last_1m_previous_year_data = None
        self.googlesearchconsole_query_previous_15m_data = None
        self.googlesearchconsole_query_previous_1m_data = None
        # screamingfrog
        self.screamingfrog_list_crawl_data = None
        self.screamingfrog_sitemap_crawl_data = None
        self.screamingfrog_spider_crawl_data = None
        # semrush
        self.semrush_analytics_backlinks_rootdomain_data = None
        self.semrush_analytics_organic_competitors_data = None
        self.semrush_analytics_organic_pages_data = None
        self.semrush_analytics_organic_positions_rootdomain_data = None
        # sitebulb
        self.sitebulb_list_crawl_url_internal_data = None
        self.sitebulb_spider_crawl_url_internal_data = None

    def run(self):
        """
        Main method to run the workflow.
        """
        logger.info(f"Starting data processing for project: {self._project.name}")
        self._retrieve_export_data()
        self._process_data()
        self._store_data_in_database()
        logger.info(f"Data processing completed for project: {self._project.name}")

    def _retrieve_export_data(self):
        """
        Retrieves data from various exports.
        """
        # googleanalytics4
        googleanalytics4_last_14m_export = GoogleAnalytics4Last14mExport(self._project)
        self.googleanalytics4_last_14m_data = googleanalytics4_last_14m_export.get_data()

        googleanalytics4_last_1m_export = GoogleAnalytics4Last1mExport(self._project)
        self.googleanalytics4_last_1m_data = googleanalytics4_last_1m_export.get_data()

        googleanalytics4_last_1m_previous_year_export = GoogleAnalytics4Last1mPreviousYearExport(self._project)
        self.googleanalytics4_last_1m_previous_year_data = googleanalytics4_last_1m_previous_year_export.get_data()

        googleanalytics4_previous_1m_export = GoogleAnalytics4Previous1mExport(self._project)
        self.googleanalytics4_previous_1m_data = googleanalytics4_previous_1m_export.get_data()

        # googlesearchconsole
        googlesearchconsole_date_page_query_last_16m_export = GoogleSearchConsoleDatePageQueryLast16mExport(self._project)
        self.googlesearchconsole_date_page_query_last_16m_data = googlesearchconsole_date_page_query_last_16m_export.get_data()

        googlesearchconsole_page_last_16m_export = GoogleSearchConsolePageLast16mExport(self._project)
        self.googlesearchconsole_page_last_16m_data = googlesearchconsole_page_last_16m_export.get_data()

        googlesearchconsole_page_last_1m_export = GoogleSearchConsolePageLast1mExport(self._project)
        self.googlesearchconsole_page_last_1m_data = googlesearchconsole_page_last_1m_export.get_data()

        googlesearchconsole_page_last_1m_previous_year_export = GoogleSearchConsolePageLast1mPreviousYearExport(self._project)
        self.googlesearchconsole_page_last_1m_previous_year_data = googlesearchconsole_page_last_1m_previous_year_export.get_data()

        googlesearchconsole_page_previous_1m_export = GoogleSearchConsolePagePrevious1mExport(self._project)
        self.googlesearchconsole_page_previous_1m_data = googlesearchconsole_page_previous_1m_export.get_data()

        googlesearchconsole_page_query_last_16m_export = GoogleSearchConsolePageQueryLast16mExport(self._project)
        self.googlesearchconsole_page_query_last_16m_data = googlesearchconsole_page_query_last_16m_export.get_data()

        googlesearchconsole_page_query_last_1m_export = GoogleSearchConsolePageQueryLast1mExport(self._project)
        self.googlesearchconsole_page_query_last_1m_data = googlesearchconsole_page_query_last_1m_export.get_data()

        googlesearchconsole_page_query_last_1m_previous_year_export = GoogleSearchConsolePageQueryLast1mPreviousYearExport(self._project)
        self.googlesearchconsole_page_query_last_1m_previous_year_data = googlesearchconsole_page_query_last_1m_previous_year_export.get_data()

        googlesearchconsole_page_query_previous_1m_export = GoogleSearchConsolePageQueryPrevious1mExport(self._project)
        self.googlesearchconsole_page_query_previous_1m_data = googlesearchconsole_page_query_previous_1m_export.get_data()

        googlesearchconsole_query_last_16m_export = GoogleSearchConsoleQueryLast16mExport(self._project)
        self.googlesearchconsole_query_last_16m_data = googlesearchconsole_query_last_16m_export.get_data()

        googlesearchconsole_query_last_1m_export = GoogleSearchConsoleQueryLast1mExport(self._project)
        self.googlesearchconsole_query_last_1m_data = googlesearchconsole_query_last_1m_export.get_data()

        googlesearchconsole_query_last_1m_previous_year_export = GoogleSearchConsoleQueryLast1mPreviousYearExport(self._project)
        self.googlesearchconsole_query_last_1m_previous_year_data = googlesearchconsole_query_last_1m_previous_year_export.get_data()

        googlesearchconsole_query_previous_15m_export = GoogleSearchConsoleQueryPrevious15mExport(self._project)
        self.googlesearchconsole_query_previous_15m_data = googlesearchconsole_query_previous_15m_export.get_data()

        googlesearchconsole_query_previous_1m_export = GoogleSearchConsoleQueryPrevious1mExport(self._project)
        self.googlesearchconsole_query_previous_1m_data = googlesearchconsole_query_previous_1m_export.get_data()

        # screamingfrog
        screamingfrog_list_crawl_export = ScreamingFrogListCrawlExport(self._project)
        self.screamingfrog_list_crawl_data = screamingfrog_list_crawl_export.get_data()

        screamingfrog_sitemap_crawl_export = ScreamingFrogSitemapCrawlExport(self._project)
        self.screamingfrog_sitemap_crawl_data = screamingfrog_sitemap_crawl_export.get_data()

        screamingfrog_spider_crawl_export = ScreamingFrogSpiderCrawlExport(self._project)
        self.screamingfrog_spider_crawl_data = screamingfrog_spider_crawl_export.get_data()

        # semrush
        semrush_analytics_backlinks_rootdomain_export = SemrushAnalyticsBacklinksRootdomainExport(self._project)
        self.semrush_analytics_backlinks_rootdomain_data = semrush_analytics_backlinks_rootdomain_export.get_data()

        semrush_analytics_organic_competitors_export = SemrushAnalyticsOrganicCompetitorsExport(self._project)
        self.semrush_analytics_organic_competitors_data = semrush_analytics_organic_competitors_export.get_data()

        semrush_analytics_organic_pages_export = SemrushAnalyticsOrganicPagesExport(self._project)
        self.semrush_analytics_organic_pages_data = semrush_analytics_organic_pages_export.get_data()

        semrush_analytics_organic_positions_rootdomain_export = SemrushAnalyticsOrganicPositionsRootdomainExport(self._project)
        self.semrush_analytics_organic_positions_rootdomain_data = semrush_analytics_organic_positions_rootdomain_export.get_data()

        # sitebulb
        sitebulb_list_crawl_url_internal_export = SitebulbListCrawlUrlInternalExport(self._project)
        self.sitebulb_list_crawl_url_internal_data = sitebulb_list_crawl_url_internal_export.get_data()

        sitebulb_spider_crawl_url_internal_export = SitebulbSpiderCrawlUrlInternalExport(self._project)
        self.sitebulb_spider_crawl_url_internal_data = sitebulb_spider_crawl_url_internal_export.get_data()

    def _process_data(self):
        """
        Processes the retrieved data.
        """
        self._process_url_data()

    # data process methods
    def _process_url_data(self):
        # use list crawl data as base
        screamingfrog_list_crawl_data_columns = [
            "Address",
            "Status Code",
            "Redirect URL",
            "Content Type",
            "Indexability",
            "Indexability Status",
            "Title 1",
            "Meta Description 1",
            "H1-1",
            "Canonical Link Element 1",
            "Word Count",
            "Readability",
            "Unique Inlinks",
            "Unique Outlinks",
            "Hash",
            "Crawl Timestamp",
        ]
        df = self.screamingfrog_list_crawl_data[screamingfrog_list_crawl_data_columns]

        # join spider crawl data
        screamingfrog_spider_crawl_data_columns = [
            "Address",
            "Crawl Depth",
        ]
        df = pd.merge(df, self.screamingfrog_spider_crawl_data[screamingfrog_spider_crawl_data_columns], left_on="Address", right_on="Address", how="left")

        # join sitemap crawl data
        self.screamingfrog_sitemap_crawl_data["In Sitemap"] = True
        screamingfrog_sitemap_crawl_data_columns = [
            "Address",
            "In Sitemap",
        ]
        df = pd.merge(df, self.screamingfrog_sitemap_crawl_data[screamingfrog_sitemap_crawl_data_columns], left_on="Address", right_on="Address", how="left")
        df["In Sitemap"].fillna(False, inplace=True)

        # join sitebulb crawl data
        sitebulb_list_crawl_url_internal_data_columns = [
            "URL",
            "HTML Template",
            "No. Content Words",
            "No. Template Words",
            "No. Words",
        ]
        df = pd.merge(df, self.sitebulb_list_crawl_url_internal_data[sitebulb_list_crawl_url_internal_data_columns], left_on="Address", right_on="URL", how="left")

        # calculate relative url column
        df["Path"] = df["Address"].apply(UrlManager.get_relative_url)

        # Generic approach to handle NaN values based on column data type
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col].fillna(0, inplace=True)  # Replace NaN with 0 for numeric columns
            else:
                df[col].fillna("", inplace=True)  # Replace NaN with empty string for non-numeric columns

        self._url_df = df

    def _store_data_in_database(self):
        """
        Stores the processed data in the database.
        """
        self._store_url_data()

    def _store_url_data(self):
        website = WebsiteManager.get_website_by_project(self._project)
        for _, row in self._url_df.iterrows():
            # Extract required data from the row
            full_address = row["Address"]

            # Use UrlManager's method to push the URL to the database
            UrlManager.push_url(
                full_address=full_address,
                website=website,
                status_code=row["Status Code"],
                redirect_url=row["Redirect URL"],
                content_type=row["Content Type"],
                canonical_link_element_1=row["Canonical Link Element 1"],
                content_words_count=row["No. Content Words"],
                crawl_timestamp=row["Crawl Timestamp"],
                h1_1=row["H1-1"],
                hash=row["Hash"],
                in_sitemap=row["In Sitemap"],
                crawl_depth=row["Crawl Depth"],
                html_template=row["HTML Template"],
                indexability=row["Indexability"],
                indexability_status=row["Indexability Status"],
                meta_description_1=row["Meta Description 1"],
                readability=row["Readability"],
                relative_address=row["Path"],
                template_words_count=row["No. Template Words"],
                title_1=row["Title 1"],
                unique_inlinks=row["Unique Inlinks"],
                unique_outlinks=row["Unique Outlinks"],
                word_count=row["Word Count"],
                word_count2=row["No. Words"],
            )
        logger.info("Data successfully stored in database using UrlManager.")
