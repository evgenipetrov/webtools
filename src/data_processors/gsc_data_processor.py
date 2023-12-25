import logging

import pandas as pd
from pandas import isna

from core.models import Project
from core.models.gscpage import GscPageManager
from core.models.gscquery import GscQueryManager
from core.models.url import UrlManager
from core.models.website import WebsiteManager
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

logger = logging.getLogger(__name__)


def aggregate_gscpage_data(df):
    return df.groupby("redirect_url").agg({"impressions": "sum", "clicks": "sum", "ctr": "mean", "position": "mean"})


def get_aggregated_gscpage_data_for_url(agg_data, redirect_url, default_value=0):
    return agg_data.loc[redirect_url] if redirect_url in agg_data.index else {"impressions": default_value, "clicks": default_value, "ctr": default_value, "position": default_value}


class GscDataProcessor:
    def __init__(self, project: Project):
        self._project = project
        self._website = WebsiteManager.get_website_by_project(project)

        self._gscpage_data = None
        self._gscquery_data = None

    def run(self):
        self.collect_data()
        self.process_gscpage_data()
        self.process_gscquery_data()
        self.store_data()

    def collect_data(self):
        googlesearchconsole_page_last_1m_export = GoogleSearchConsolePageLast1mExport(self._project)
        self.googlesearchconsole_page_last_1m_data = googlesearchconsole_page_last_1m_export.get_data()

        googlesearchconsole_page_last_16m_export = GoogleSearchConsolePageLast16mExport(self._project)
        self.googlesearchconsole_page_last_16m_data = googlesearchconsole_page_last_16m_export.get_data()

        googlesearchconsole_page_previous_1m_export = GoogleSearchConsolePagePrevious1mExport(self._project)
        self.googlesearchconsole_page_previous_1m_data = googlesearchconsole_page_previous_1m_export.get_data()

        googlesearchconsole_page_last_1m_previous_year_export = GoogleSearchConsolePageLast1mPreviousYearExport(self._project)
        self.googlesearchconsole_page_last_1m_previous_year_data = googlesearchconsole_page_last_1m_previous_year_export.get_data()

        googlesearchconsole_query_last_1m_export = GoogleSearchConsoleQueryLast1mExport(self._project)
        self.googlesearchconsole_query_last_1m_data = googlesearchconsole_query_last_1m_export.get_data()

        googlesearchconsole_query_last_16m_export = GoogleSearchConsoleQueryLast16mExport(self._project)
        self.googlesearchconsole_query_last_16m_data = googlesearchconsole_query_last_16m_export.get_data()

        googlesearchconsole_query_previous_1m_export = GoogleSearchConsoleQueryPrevious1mExport(self._project)
        self.googlesearchconsole_query_previous_1m_data = googlesearchconsole_query_previous_1m_export.get_data()

        googlesearchconsole_query_previous_15m_export = GoogleSearchConsoleQueryPrevious15mExport(self._project)
        self.googlesearchconsole_query_previous_15m_data = googlesearchconsole_query_previous_15m_export.get_data()

        googlesearchconsole_query_last_1m_previous_year_export = GoogleSearchConsoleQueryLast1mPreviousYearExport(self._project)
        self.googlesearchconsole_query_last_1m_previous_year_data = googlesearchconsole_query_last_1m_previous_year_export.get_data()

        googlesearchconsole_page_query_last_1m_export = GoogleSearchConsolePageQueryLast1mExport(self._project)
        self.googlesearchconsole_page_query_last_1m_data = googlesearchconsole_page_query_last_1m_export.get_data()

        googlesearchconsole_page_query_last_16m_export = GoogleSearchConsolePageQueryLast16mExport(self._project)
        self.googlesearchconsole_page_query_last_16m_data = googlesearchconsole_page_query_last_16m_export.get_data()

        googlesearchconsole_page_query_previous_1m_export = GoogleSearchConsolePageQueryPrevious1mExport(self._project)
        self.googlesearchconsole_page_query_previous_1m_data = googlesearchconsole_page_query_previous_1m_export.get_data()

        googlesearchconsole_page_query_last_1m_previous_year_export = GoogleSearchConsolePageQueryLast1mPreviousYearExport(self._project)
        self.googlesearchconsole_page_query_last_1m_previous_year_data = googlesearchconsole_page_query_last_1m_previous_year_export.get_data()

    def process_gscpage_data(self):
        urls = UrlManager.get_urls_by_website(self._website)

        url_table = [{"full_address": url.full_address, "status_code": url.status_code, "redirect_url": url.redirect_url} for url in urls]

        df = pd.DataFrame(url_table)

        # Fill empty 'redirect_url' with 'full_address'
        df["redirect_url"] = df.apply(lambda row: row["full_address"] if pd.isna(row["redirect_url"]) or row["redirect_url"] == "" else row["redirect_url"], axis=1)

        self._gscpage_data = df

        # group data
        googlesearchconsole_page_last_1m_aggregated_data = aggregate_gscpage_data(df.merge(self.googlesearchconsole_page_last_1m_data, left_on="full_address", right_on="page", how="left"))
        googlesearchconsole_page_last_1m_aggregated_data = googlesearchconsole_page_last_1m_aggregated_data.rename(columns=lambda x: x + "_last_1m" if x != "redirect_url" else x)
        self._gscpage_data = self._gscpage_data.merge(
            googlesearchconsole_page_last_1m_aggregated_data,
            left_on="redirect_url",
            right_index=True,
            how="left",
        )

        googlesearchconsole_page_last_1m_previous_year_aggregated_data = aggregate_gscpage_data(df.merge(self.googlesearchconsole_page_last_1m_previous_year_data, left_on="full_address", right_on="page", how="left"))
        googlesearchconsole_page_last_1m_previous_year_aggregated_data = googlesearchconsole_page_last_1m_previous_year_aggregated_data.rename(columns=lambda x: x + "_last_1m_previous_year" if x != "redirect_url" else x)
        self._gscpage_data = self._gscpage_data.merge(
            googlesearchconsole_page_last_1m_previous_year_aggregated_data,
            left_on="redirect_url",
            right_index=True,
            how="left",
        )
        googlesearchconsole_page_last_16m_aggregated_data = aggregate_gscpage_data(df.merge(self.googlesearchconsole_page_last_16m_data, left_on="full_address", right_on="page", how="left"))
        googlesearchconsole_page_last_16m_aggregated_data = googlesearchconsole_page_last_16m_aggregated_data.rename(columns=lambda x: x + "_last_16m" if x != "redirect_url" else x)
        self._gscpage_data = self._gscpage_data.merge(
            googlesearchconsole_page_last_16m_aggregated_data,
            left_on="redirect_url",
            right_index=True,
            how="left",
        )
        googlesearchconsole_page_previous_1m_aggregated_data = aggregate_gscpage_data(df.merge(self.googlesearchconsole_page_previous_1m_data, left_on="full_address", right_on="page", how="left"))
        googlesearchconsole_page_previous_1m_aggregated_data = googlesearchconsole_page_previous_1m_aggregated_data.rename(columns=lambda x: x + "_previous_1m" if x != "redirect_url" else x)
        self._gscpage_data = self._gscpage_data.merge(
            googlesearchconsole_page_previous_1m_aggregated_data,
            left_on="redirect_url",
            right_index=True,
            how="left",
        )

        # googlesearchconsole_page_query_last_1m_aggregated_data = aggregate_gsc_data(df.merge(self.googlesearchconsole_page_query_last_1m_data, left_on='full_address', right_on='page', how='left'))
        # googlesearchconsole_page_query_last_16m_aggregated_data = aggregate_gsc_data(df.merge(self.googlesearchconsole_page_query_last_16m_data, left_on='full_address', right_on='page', how='left'))
        # googlesearchconsole_page_query_previous_1m_aggregated_data = aggregate_gsc_data(df.merge(self.googlesearchconsole_page_query_previous_1m_data, left_on='full_address', right_on='page', how='left'))
        # googlesearchconsole_page_query_last_1m_previous_year_aggregated_data = aggregate_gsc_data(df.merge(self.googlesearchconsole_page_query_last_1m_previous_year_data, left_on='full_address', right_on='page', how='left'))

    def process_gscquery_data(self):
        googlesearchconsole_query_last_16m_data = self.googlesearchconsole_query_last_16m_data.rename(columns=lambda x: x + "_last_16m" if x != "query" else x)
        self._gscquery_data = googlesearchconsole_query_last_16m_data

        googlesearchconsole_query_last_1m_data = self.googlesearchconsole_query_last_1m_data.rename(columns=lambda x: x + "_last_1m" if x != "query" else x)

        self._gscquery_data = self._gscquery_data.merge(
            googlesearchconsole_query_last_1m_data,
            on="query",
            how="left",
        )

        googlesearchconsole_query_previous_1m_data = self.googlesearchconsole_query_previous_1m_data.rename(columns=lambda x: x + "_previous_1m" if x != "query" else x)
        self._gscquery_data = self._gscquery_data.merge(
            googlesearchconsole_query_previous_1m_data,
            on="query",
            how="left",
        )

        googlesearchconsole_query_last_1m_previous_year_data = self.googlesearchconsole_query_last_1m_previous_year_data.rename(columns=lambda x: x + "_last_1m_previous_year" if x != "query" else x)
        self._gscquery_data = self._gscquery_data.merge(
            googlesearchconsole_query_last_1m_previous_year_data,
            on="query",
            how="left",
        )

        googlesearchconsole_query_previous_15m_data = self.googlesearchconsole_query_previous_15m_data.rename(columns=lambda x: x + "_previous_15m" if x != "query" else x)
        self._gscquery_data = self._gscquery_data.merge(googlesearchconsole_query_previous_15m_data, on="query", how="left", indicator=True)
        self._gscquery_data["in_last_1m_only"] = self._gscquery_data["_merge"] == "left_only"
        self._gscquery_data.drop(columns=["_merge"], inplace=True)

    def store_data(self):
        total_gscpages = len(self._gscpage_data)
        for index, row in self._gscpage_data.iterrows():
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
            )
            if index % 100 == 0 or index == total_gscpages:  # Log every 100 rows or on the last row
                logger.info(f"Processing GSC Page Data: Row {index} of {total_gscpages}")

        logger.info("GSC Page Data successfully processed using GscPageManager.")

        total_gscqueries = len(self._gscquery_data)
        for index, row in self._gscquery_data.iterrows():
            row = row.apply(lambda x: None if isna(x) else x)

            GscQueryManager.push_gscquery(
                query=row["query"],
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
                in_last_1m_only=row["in_last_1m_only"],
            )
            if index % 100 == 0 or index == total_gscqueries:
                logger.info(f"Processing GSC Query Data: Row {index} of {total_gscqueries}")

        logger.info("GSC Query Data successfully processed using GscPageManager.")
