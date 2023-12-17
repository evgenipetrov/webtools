import logging
from datetime import datetime

import pandas as pd
from django.utils import timezone

from core.models import Project
from core.models.url import UrlManager
from core.models.website import WebsiteManager
from exports.googleanalytics4_last_14m_export import GoogleAnalytics4Last14mExport
from exports.googlesearchconsole_page_last_16m_export import GoogleSearchConsolePageLast16mExport
from exports.screamingfrog_list_crawl_export import ScreamingFrogListCrawlExport
from exports.screamingfrog_sitemap_crawl_export import ScreamingFrogSitemapCrawlExport
from exports.screamingfrog_spider_crawl_export import ScreamingFrogSpiderCrawlExport
from exports.semrush_analytics_backlinks_rootdomain_export import SemrushAnalyticsBacklinksRootdomainExport
from exports.semrush_analytics_organic_pages_export import SemrushAnalyticsOrganicPagesExport
from exports.sitebulb_list_crawl_url_internal_export import SitebulbListCrawlUrlInternalExport

logger = logging.getLogger(__name__)


class UrlDataProcessor:
    def __init__(self, project: Project):
        self._project = project

        self._data = None

        self.sitebulb_list_crawl_url_internal_data = None
        self.semrush_analytics_backlinks_rootdomain_data = None
        self.semrush_analytics_organic_pages_data = None
        self.googlesearchconsole_page_last_16m_data = None
        self.googleanalytics4_last_14m_data = None
        self.screamingfrog_sitemap_crawl_data = None
        self.screamingfrog_spider_crawl_data = None
        self.screamingfrog_list_crawl_data = None

    def run(self):
        self.collect_data()
        self.process_data()
        self.store_data()

    def collect_data(self):
        screamingfrog_list_crawl_export = ScreamingFrogListCrawlExport(self._project)
        self.screamingfrog_list_crawl_data = screamingfrog_list_crawl_export.get_data()

        screamingfrog_spider_crawl_export = ScreamingFrogSpiderCrawlExport(self._project)
        self.screamingfrog_spider_crawl_data = screamingfrog_spider_crawl_export.get_data()

        screamingfrog_sitemap_crawl_export = ScreamingFrogSitemapCrawlExport(self._project)
        self.screamingfrog_sitemap_crawl_data = screamingfrog_sitemap_crawl_export.get_data()

        googleanalytics4_last_14m_export = GoogleAnalytics4Last14mExport(self._project)
        self.googleanalytics4_last_14m_data = googleanalytics4_last_14m_export.get_data()

        googlesearchconsole_page_last_16m_export = GoogleSearchConsolePageLast16mExport(self._project)
        self.googlesearchconsole_page_last_16m_data = googlesearchconsole_page_last_16m_export.get_data()

        semrush_analytics_organic_pages_export = SemrushAnalyticsOrganicPagesExport(self._project)
        self.semrush_analytics_organic_pages_data = semrush_analytics_organic_pages_export.get_data()

        semrush_analytics_backlinks_rootdomain_export = SemrushAnalyticsBacklinksRootdomainExport(self._project)
        self.semrush_analytics_backlinks_rootdomain_data = semrush_analytics_backlinks_rootdomain_export.get_data()

        sitebulb_list_crawl_url_internal_export = SitebulbListCrawlUrlInternalExport(self._project)
        self.sitebulb_list_crawl_url_internal_data = sitebulb_list_crawl_url_internal_export.get_data()

    def process_data(self):
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
        df = self.screamingfrog_list_crawl_data[screamingfrog_list_crawl_data_columns].drop_duplicates(subset="Address", keep="first")

        # calculate relative url column
        df["Path"] = df["Address"].apply(UrlManager.parse_relative_url)

        # join spider crawl data
        self.screamingfrog_spider_crawl_data["In Crawl"] = True
        screamingfrog_spider_crawl_data_columns = [
            "Address",
            "Crawl Depth",
            "In Crawl",
        ]
        df = pd.merge(
            df,
            self.screamingfrog_spider_crawl_data[screamingfrog_spider_crawl_data_columns].drop_duplicates(subset="Address", keep="first"),
            left_on="Address",
            right_on="Address",
            how="left",
        )
        df["In Crawl"].fillna(False, inplace=True)

        # join sitemap crawl data
        self.screamingfrog_sitemap_crawl_data["In Sitemap"] = True
        screamingfrog_sitemap_crawl_data_columns = [
            "Address",
            "In Sitemap",
        ]
        df = pd.merge(
            df,
            self.screamingfrog_sitemap_crawl_data[screamingfrog_sitemap_crawl_data_columns].drop_duplicates(subset="Address", keep="first"),
            left_on="Address",
            right_on="Address",
            how="left",
        )
        df["In Sitemap"].fillna(False, inplace=True)

        self.googleanalytics4_last_14m_data["In GA4"] = True
        googleanalytics4_last_14m_data_columns = [
            "pagePath",
            "In GA4",
        ]
        df = pd.merge(
            df,
            self.googleanalytics4_last_14m_data[googleanalytics4_last_14m_data_columns].drop_duplicates(subset="pagePath", keep="first"),
            left_on="Path",
            right_on="pagePath",
            how="left",
        )
        df["In GA4"].fillna(False, inplace=True)

        self.googlesearchconsole_page_last_16m_data["In GSC"] = True
        googlesearchconsole_page_last_16m_data_columns = [
            "page",
            "In GSC",
        ]
        df = pd.merge(
            df,
            self.googlesearchconsole_page_last_16m_data[googlesearchconsole_page_last_16m_data_columns].drop_duplicates(subset="page", keep="first"),
            left_on="Address",
            right_on="page",
            how="left",
        )
        df["In GSC"].fillna(False, inplace=True)

        self.semrush_analytics_organic_pages_data["In Semrush Pages"] = True
        semrush_analytics_organic_pages_data_columns = [
            "URL",
            "In Semrush Pages",
        ]
        df = pd.merge(
            df,
            self.semrush_analytics_organic_pages_data[semrush_analytics_organic_pages_data_columns].drop_duplicates(subset="URL", keep="first"),
            left_on="Address",
            right_on="URL",
            how="left",
        )
        df["In Semrush Pages"].fillna(False, inplace=True)
        df.drop("URL", axis=1, inplace=True)

        self.semrush_analytics_backlinks_rootdomain_data["In Semrush Backlinks"] = True
        semrush_analytics_backlinks_rootdomain_data_columns = [
            "Target url",
            "In Semrush Backlinks",
        ]
        df = pd.merge(
            df,
            self.semrush_analytics_backlinks_rootdomain_data[semrush_analytics_backlinks_rootdomain_data_columns].drop_duplicates(subset="Target url", keep="first"),
            left_on="Address",
            right_on="Target url",
            how="left",
        )
        df["In Semrush Backlinks"].fillna(False, inplace=True)
        df.drop("Target url", axis=1, inplace=True)

        # join sitebulb crawl data
        sitebulb_list_crawl_url_internal_data_columns = [
            "URL",
            "HTML Template",
            "No. Content Words",
            "No. Template Words",
            "No. Words",
        ]
        df = pd.merge(
            df,
            self.sitebulb_list_crawl_url_internal_data[sitebulb_list_crawl_url_internal_data_columns].drop_duplicates(subset="URL", keep="first"),
            left_on="Address",
            right_on="URL",
            how="left",
        )
        df.drop("URL", axis=1, inplace=True)

        # Generic approach to handle NaN values based on column data type
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col].fillna(0, inplace=True)  # Replace NaN with 0 for numeric columns
            else:
                df[col].fillna("", inplace=True)  # Replace NaN with empty string for non-numeric columns

        self._data = df

    def store_data(self):
        website = WebsiteManager.get_website_by_project(self._project)
        for _, row in self._data.iterrows():
            # Extract required data from the row
            full_address = row["Address"]

            #
            # Parse the string to a naive datetime object
            naive_datetime = datetime.strptime(row["Crawl Timestamp"], "%Y-%m-%d %H:%M:%S")

            # Convert the naive datetime to a timezone-aware datetime
            aware_datetime = timezone.make_aware(naive_datetime, timezone.get_default_timezone())

            # Use UrlManager's method to push the URL to the database
            UrlManager.push_url(
                full_address=full_address,
                website=website,
                status_code=row["Status Code"],
                redirect_url=row["Redirect URL"],
                content_type=row["Content Type"],
                canonical_link_element_1=row["Canonical Link Element 1"],
                content_words_count=row["No. Content Words"],
                crawl_timestamp=aware_datetime,
                h1_1=row["H1-1"],
                hash=row["Hash"],
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
                in_sitemap=row["In Sitemap"],
                in_crawl=row["In Crawl"],
                in_ga4=row["In GA4"],
                in_gsc=row["In GSC"],
                in_semrush_pages=row["In Semrush Pages"],
                in_semrush_backlinks=row["In Semrush Backlinks"],
            )
        logger.info("Data successfully processed using UrlManager.")
