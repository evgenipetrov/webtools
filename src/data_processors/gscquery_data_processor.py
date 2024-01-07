import logging

import numpy as np
import pandas as pd
from pandas import isna

from core.models import Project
from core.models.gscquery import GscQueryManager
from core.models.keyword import KeywordManager
from core.models.website import WebsiteManager
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


class GscQueryDataProcessor:
    def __init__(self, project: Project):
        self._project = project
        self._website = WebsiteManager.get_website_by_project(project)

        self._data = None

    def run(self):
        self.collect_data()
        self.process_data()
        self.store_data()

    def collect_data(self):
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

    def process_data(self):
        # stack key column then map
        df = pd.concat(
            [
                self.googlesearchconsole_query_last_16m_data["query"],
            ]
        )
        df = pd.DataFrame(df.unique(), columns=["query"])

        googlesearchconsole_query_last_16m_data = self.googlesearchconsole_query_last_16m_data.rename(columns=lambda x: x + "_last_16m" if x != "query" else x)
        df = googlesearchconsole_query_last_16m_data

        googlesearchconsole_query_last_1m_data = self.googlesearchconsole_query_last_1m_data.rename(columns=lambda x: x + "_last_1m" if x != "query" else x)

        df = df.merge(
            googlesearchconsole_query_last_1m_data,
            on="query",
            how="left",
        )

        googlesearchconsole_query_previous_1m_data = self.googlesearchconsole_query_previous_1m_data.rename(columns=lambda x: x + "_previous_1m" if x != "query" else x)
        df = df.merge(
            googlesearchconsole_query_previous_1m_data,
            on="query",
            how="left",
        )

        googlesearchconsole_query_last_1m_previous_year_data = self.googlesearchconsole_query_last_1m_previous_year_data.rename(columns=lambda x: x + "_last_1m_previous_year" if x != "query" else x)
        if not googlesearchconsole_query_last_1m_previous_year_data.empty:
            df = df.merge(
                googlesearchconsole_query_last_1m_previous_year_data,
                on="query",
                how="left",
            )
        else:
            df["impressions_last_1m_previous_year"] = np.nan
            df["clicks_last_1m_previous_year"] = np.nan
            df["ctr_last_1m_previous_year"] = np.nan
            df["position_last_1m_previous_year"] = np.nan

        googlesearchconsole_query_previous_15m_data = self.googlesearchconsole_query_previous_15m_data.rename(columns=lambda x: x + "_previous_15m" if x != "query" else x)
        df = df.merge(googlesearchconsole_query_previous_15m_data, on="query", how="left", indicator=True)
        df["in_last_1m_only"] = df["_merge"] == "left_only"
        df.drop(columns=["_merge"], inplace=True)

        self._data = df

    def store_data(self):
        total_rows = len(self._data)
        for index, row in self._data.iterrows():
            row = row.apply(lambda x: None if isna(x) else x)

            keyword = KeywordManager.push_keyword(row["query"])

            GscQueryManager.push_gscquery(
                keyword=keyword,
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
            if index % 100 == 0 or index == total_rows:
                logger.info(f"GscQueryManager: Processing GSC Query Data: Row {index} of {total_rows} ({(index / total_rows) * 100:.2f}% complete)")

        logger.info("GSC Query Data successfully processed using GscPageManager.")
