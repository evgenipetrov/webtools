import logging

import pandas as pd
from pandas import isna

from core.models import Project
from core.models.gscpage import GscPageManager
from core.models.url import UrlManager
from core.models.website import WebsiteManager
from exports.googleanalytics4_last_14m_export import GoogleAnalytics4Last14mExport
from exports.googlesearchconsole_page_last_16m_export import GoogleSearchConsolePageLast16mExport
from exports.googlesearchconsole_page_last_1m_export import GoogleSearchConsolePageLast1mExport
from exports.googlesearchconsole_page_last_1m_previous_year_export import GoogleSearchConsolePageLast1mPreviousYearExport
from exports.googlesearchconsole_page_previous_1m_export import GoogleSearchConsolePagePrevious1mExport
from exports.googlesearchconsole_page_query_last_16m_export import GoogleSearchConsolePageQueryLast16mExport
from exports.googlesearchconsole_page_query_last_1m_export import GoogleSearchConsolePageQueryLast1mExport
from exports.googlesearchconsole_page_query_last_1m_previous_year_export import GoogleSearchConsolePageQueryLast1mPreviousYearExport
from exports.googlesearchconsole_page_query_previous_1m_export import GoogleSearchConsolePageQueryPrevious1mExport
from exports.screamingfrog_list_crawl_export import ScreamingFrogListCrawlManualExport
from exports.screamingfrog_sitemap_crawl_export import ScreamingFrogSitemapCrawlManualExport
from exports.screamingfrog_spider_crawl_export import ScreamingFrogSpiderCrawlManualExport
from exports.semrush_analytics_backlinks_rootdomain_export import SemrushAnalyticsBacklinksRootdomainExport
from exports.semrush_analytics_organic_pages_export import SemrushAnalyticsOrganicPagesExport
from exports.sitebulb_list_crawl_url_internal_export import SitebulbListCrawlUrlInternalExport

logger = logging.getLogger(__name__)


def aggregate_gscpage_data(df):
    df["Redirect URL"].fillna(df["full_address"], inplace=True)
    aggregated_df = df.groupby("Redirect URL").agg({"impressions": "sum", "clicks": "sum", "ctr": "mean", "position": "mean"})
    return aggregated_df


def aggregate_gscpagequery_data(df):
    df["Redirect URL"].fillna(df["full_address"], inplace=True)
    aggregated_df = df.groupby("Redirect URL").agg({"query": "count"})
    aggregated_df.rename(columns={"query": "querycount"}, inplace=True)
    return aggregated_df


class GscPageDataProcessor:
    def __init__(self, project: Project):
        self._project = project
        self._website = WebsiteManager.get_website_by_project(project)

        self._data = None

    def run(self):
        self.collect_data()
        self.process_data()
        self.store_data()

    def collect_data(self):
        screamingfrog_list_crawl_export = ScreamingFrogListCrawlManualExport(self._project)
        self.screamingfrog_list_crawl_data = screamingfrog_list_crawl_export.get_data()

        googlesearchconsole_page_last_16m_export = GoogleSearchConsolePageLast16mExport(self._project)
        self.googlesearchconsole_page_last_16m_data = googlesearchconsole_page_last_16m_export.get_data()

        googlesearchconsole_page_last_1m_export = GoogleSearchConsolePageLast1mExport(self._project)
        self.googlesearchconsole_page_last_1m_data = googlesearchconsole_page_last_1m_export.get_data()

        googlesearchconsole_page_previous_1m_export = GoogleSearchConsolePagePrevious1mExport(self._project)
        self.googlesearchconsole_page_previous_1m_data = googlesearchconsole_page_previous_1m_export.get_data()

        googlesearchconsole_page_last_1m_previous_year_export = GoogleSearchConsolePageLast1mPreviousYearExport(self._project)
        self.googlesearchconsole_page_last_1m_previous_year_data = googlesearchconsole_page_last_1m_previous_year_export.get_data()

        googlesearchconsole_page_query_last_16m_export = GoogleSearchConsolePageQueryLast16mExport(self._project)
        self.googlesearchconsole_page_query_last_16m_data = googlesearchconsole_page_query_last_16m_export.get_data()

        googlesearchconsole_page_query_last_1m_export = GoogleSearchConsolePageQueryLast1mExport(self._project)
        self.googlesearchconsole_page_query_last_1m_data = googlesearchconsole_page_query_last_1m_export.get_data()

        googlesearchconsole_page_query_previous_1m_export = GoogleSearchConsolePageQueryPrevious1mExport(self._project)
        self.googlesearchconsole_page_query_previous_1m_data = googlesearchconsole_page_query_previous_1m_export.get_data()

        googlesearchconsole_page_query_last_1m_previous_year_export = GoogleSearchConsolePageQueryLast1mPreviousYearExport(self._project)
        self.googlesearchconsole_page_query_last_1m_previous_year_data = googlesearchconsole_page_query_last_1m_previous_year_export.get_data()

    def process_data(self):
        # stack key column then map
        df = pd.concat(
            [
                self.googlesearchconsole_page_last_16m_data["page"],
            ]
        )

        df = pd.DataFrame(df.unique(), columns=["full_address"])
        # Drop rows where the URL has a fragment
        mask = df["full_address"].apply(UrlManager.has_fragment)
        df = df[~mask]

        screamingfrog_list_crawl_data_columns = [
            "Address",
            "Status Code",
            "Redirect URL",
        ]
        df = pd.merge(
            df,
            self.screamingfrog_list_crawl_data[screamingfrog_list_crawl_data_columns].drop_duplicates(subset="Address", keep="first"),
            left_on="full_address",
            right_on="Address",
            how="left",
        )

        # group data
        googlesearchconsole_page_last_1m_aggregated_data = aggregate_gscpage_data(df.merge(self.googlesearchconsole_page_last_1m_data, left_on="full_address", right_on="page", how="left"))
        googlesearchconsole_page_last_1m_aggregated_data = googlesearchconsole_page_last_1m_aggregated_data.rename(columns=lambda x: x + "_last_1m" if x != "redirect_url" else x)
        df = df.merge(
            googlesearchconsole_page_last_1m_aggregated_data,
            left_on="Redirect URL",
            right_index=True,
            how="left",
        )
        googlesearchconsole_page_query_last_1m_aggregated_data = aggregate_gscpagequery_data(df.merge(self.googlesearchconsole_page_query_last_1m_data, left_on="full_address", right_on="page", how="left"))
        googlesearchconsole_page_query_last_1m_aggregated_data = googlesearchconsole_page_query_last_1m_aggregated_data.rename(columns=lambda x: x + "_last_1m" if x != "redirect_url" else x)
        df = df.merge(
            googlesearchconsole_page_query_last_1m_aggregated_data,
            left_on="Redirect URL",
            right_index=True,
            how="left",
        )

        googlesearchconsole_page_last_1m_previous_year_aggregated_data = aggregate_gscpage_data(df.merge(self.googlesearchconsole_page_last_1m_previous_year_data, left_on="full_address", right_on="page", how="left"))
        googlesearchconsole_page_last_1m_previous_year_aggregated_data = googlesearchconsole_page_last_1m_previous_year_aggregated_data.rename(columns=lambda x: x + "_last_1m_previous_year" if x != "redirect_url" else x)
        df = df.merge(
            googlesearchconsole_page_last_1m_previous_year_aggregated_data,
            left_on="Redirect URL",
            right_index=True,
            how="left",
        )
        googlesearchconsole_page_query_last_1m_previous_year_aggregated_data = aggregate_gscpagequery_data(df.merge(self.googlesearchconsole_page_query_last_1m_previous_year_data, left_on="full_address", right_on="page", how="left"))
        googlesearchconsole_page_query_last_1m_previous_year_aggregated_data = googlesearchconsole_page_query_last_1m_previous_year_aggregated_data.rename(columns=lambda x: x + "_last_1m_previous_year" if x != "redirect_url" else x)
        df = df.merge(
            googlesearchconsole_page_query_last_1m_previous_year_aggregated_data,
            left_on="Redirect URL",
            right_index=True,
            how="left",
        )

        googlesearchconsole_page_last_16m_aggregated_data = aggregate_gscpage_data(df.merge(self.googlesearchconsole_page_last_16m_data, left_on="full_address", right_on="page", how="left"))
        googlesearchconsole_page_last_16m_aggregated_data = googlesearchconsole_page_last_16m_aggregated_data.rename(columns=lambda x: x + "_last_16m" if x != "redirect_url" else x)
        df = df.merge(
            googlesearchconsole_page_last_16m_aggregated_data,
            left_on="Redirect URL",
            right_index=True,
            how="left",
        )
        googlesearchconsole_page_query_last_16m_aggregated_data = aggregate_gscpagequery_data(df.merge(self.googlesearchconsole_page_query_last_16m_data, left_on="full_address", right_on="page", how="left"))
        googlesearchconsole_page_query_last_16m_aggregated_data = googlesearchconsole_page_query_last_16m_aggregated_data.rename(columns=lambda x: x + "_last_16m" if x != "redirect_url" else x)
        df = df.merge(
            googlesearchconsole_page_query_last_16m_aggregated_data,
            left_on="Redirect URL",
            right_index=True,
            how="left",
        )

        googlesearchconsole_page_previous_1m_aggregated_data = aggregate_gscpage_data(df.merge(self.googlesearchconsole_page_previous_1m_data, left_on="full_address", right_on="page", how="left"))
        googlesearchconsole_page_previous_1m_aggregated_data = googlesearchconsole_page_previous_1m_aggregated_data.rename(columns=lambda x: x + "_previous_1m" if x != "redirect_url" else x)
        df = df.merge(
            googlesearchconsole_page_previous_1m_aggregated_data,
            left_on="Redirect URL",
            right_index=True,
            how="left",
        )
        googlesearchconsole_page_query_previous_1m_aggregated_data = aggregate_gscpagequery_data(df.merge(self.googlesearchconsole_page_query_previous_1m_data, left_on="full_address", right_on="page", how="left"))
        googlesearchconsole_page_query_previous_1m_aggregated_data = googlesearchconsole_page_query_previous_1m_aggregated_data.rename(columns=lambda x: x + "_previous_1m" if x != "redirect_url" else x)
        df = df.merge(
            googlesearchconsole_page_query_previous_1m_aggregated_data,
            left_on="Redirect URL",
            right_index=True,
            how="left",
        )

        self._data = df

    def store_data(self):
        total_rows = len(self._data)
        for index, row in self._data.iterrows():
            row = row.apply(lambda x: None if isna(x) else x)

            GscPageManager.push_gscpage(
                full_address=row["full_address"],
                website=self._website,
                impressions_last_1m=row["impressions_last_1m"],
                impressions_last_16m=row["impressions_last_16m"],
                impressions_last_1m_previous_year=row["impressions_last_1m_previous_year"],
                impressions_previous_1m=row["impressions_previous_1m"],
                clicks_last_1m=row["clicks_last_1m"],
                clicks_last_16m=row["clicks_last_16m"],
                clicks_last_1m_previous_year=row["clicks_last_1m_previous_year"],
                clicks_previous_1m=row["clicks_previous_1m"],
                ctr_last_1m=row["ctr_last_1m"],
                ctr_last_16m=row["ctr_last_16m"],
                ctr_last_1m_previous_year=row["ctr_last_1m_previous_year"],
                ctr_previous_1m=row["ctr_previous_1m"],
                position_last_1m=row["position_last_1m"],
                position_last_16m=row["position_last_16m"],
                position_last_1m_previous_year=row["position_last_1m_previous_year"],
                position_previous_1m=row["position_previous_1m"],
                querycount_last_1m=row["querycount_last_1m"],
                querycount_last_16m=row["querycount_last_16m"],
                querycount_last_1m_previous_year=row["querycount_last_1m_previous_year"],
                querycount_previous_1m=row["querycount_previous_1m"],
            )
            if index % 100 == 0 or index == total_rows:  # Log every 100 rows or on the last row
                logger.info(f"GscPageManager: Processing GSC Page Data: Row {index} of {total_rows} ({(index / total_rows) * 100:.2f}% complete)")

        logger.info("GSC Page Data successfully processed using GscPageManager.")
