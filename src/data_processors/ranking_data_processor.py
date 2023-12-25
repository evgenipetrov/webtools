import logging
from datetime import datetime

import pandas as pd
from django.utils import timezone
from pandas import isna

from core.models import Project
from core.models.keyword import KeywordManager
from core.models.ranking import RankingManager
from core.models.url import UrlManager
from core.models.website import WebsiteManager
from exports.googlesearchconsole_page_query_last_1m_export import GoogleSearchConsolePageQueryLast1mExport
from exports.googlesearchconsole_page_query_last_1m_previous_year_export import GoogleSearchConsolePageQueryLast1mPreviousYearExport
from exports.googlesearchconsole_page_query_previous_1m_export import GoogleSearchConsolePageQueryPrevious1mExport
from exports.googlesearchconsole_query_last_16m_export import GoogleSearchConsoleQueryLast16mExport
from exports.semrush_analytics_organic_positions_rootdomain import SemrushAnalyticsOrganicPositionsRootdomainExport

logger = logging.getLogger(__name__)


class RankingDataProcessor:
    def __init__(self, project: Project):
        self._project = project
        self._website = WebsiteManager.get_website_by_project(project)

        self._data = None

    def run(self):
        self.collect_data()
        self.process_data()
        self.store_data()

    def collect_data(self):
        googlesearchconsole_query_last_16m_export = GoogleSearchConsoleQueryLast16mExport(self._project)
        self.googlesearchconsole_query_last_16m_data = googlesearchconsole_query_last_16m_export.get_data()

        semrush_analytics_organic_positions_rootdomain_export = SemrushAnalyticsOrganicPositionsRootdomainExport(self._project)
        self.semrush_analytics_organic_positions_rootdomain_data = semrush_analytics_organic_positions_rootdomain_export.get_data()

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
                self.semrush_analytics_organic_positions_rootdomain_data["Keyword"],
                self.googlesearchconsole_query_last_16m_data["query"],
            ]
        )
        df = pd.DataFrame(df.unique(), columns=["keyword"])

        semrush_analytics_organic_positions_rootdomain_data = self.semrush_analytics_organic_positions_rootdomain_data.rename(columns=lambda x: x + " (Semrush)" if x != "Keyword" else x)
        semrush_analytics_organic_positions_rootdomain_data_columns = [
            "Keyword",
            "Position (Semrush)",
            "Previous position (Semrush)",
            "URL (Semrush)",
            "Timestamp (Semrush)",
            "Position Type (Semrush)",
        ]
        df = pd.merge(
            df,
            semrush_analytics_organic_positions_rootdomain_data[semrush_analytics_organic_positions_rootdomain_data_columns],
            left_on="keyword",
            right_on="Keyword",
            how="left",
        )
        df.drop("Keyword", axis=1, inplace=True)

        self._data = df

    def store_data(self):
        total_rows = len(self._data)
        for index, row in self._data.iterrows():
            row = row.apply(lambda x: None if isna(x) else x)

            keyword = KeywordManager.push_keyword(row["keyword"])
            if row["URL (Semrush)"] is not None:
                semrush_url = UrlManager.push_url(row["URL (Semrush)"], self._website)
                # Parse the string to a naive datetime object
                naive_datetime = datetime.strptime(row["Timestamp (Semrush)"], "%Y-%m-%d")
                semrush_timestamp = timezone.make_aware(naive_datetime, timezone.get_default_timezone())
            else:
                semrush_url = None
                semrush_timestamp = None

            # Use UrlManager's method to push the URL to the database
            RankingManager.push_ranking(
                keyword=keyword,
                website=self._website,
                semrush_url=semrush_url,
                semrush_current_position=row["Position (Semrush)"],
                semrush_previous_position=row["Previous position (Semrush)"],
                semrush_timestamp=semrush_timestamp,
                semrush_position_type=row["Position Type (Semrush)"],
            )
            if index % 100 == 0 or index == total_rows:
                logger.info(f"RankingManager: Processing Ranking Data: Row {index} of {total_rows} ({(index / total_rows) * 100:.2f}% complete)")

        logger.info("Data successfully processed using RankingManager.")
