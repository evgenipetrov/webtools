import logging
from datetime import datetime

import pandas as pd
from django.utils import timezone
from pandas import isna

from core.models import Project
from core.models.keyword import KeywordManager
from core.models.keywordranking import KeywordRankingManager
from core.models.url import UrlManager
from core.models.urlranking import UrlRankingManager
from core.models.website import WebsiteManager
from exports.googlesearchconsole_page_last_16m_export import GoogleSearchConsolePageLast16mExport
from exports.googlesearchconsole_page_query_last_16m_export import GoogleSearchConsolePageQueryLast16mExport
from exports.googlesearchconsole_page_query_last_1m_export import GoogleSearchConsolePageQueryLast1mExport
from exports.googlesearchconsole_page_query_last_1m_previous_year_export import GoogleSearchConsolePageQueryLast1mPreviousYearExport
from exports.googlesearchconsole_page_query_previous_1m_export import GoogleSearchConsolePageQueryPrevious1mExport
from exports.googlesearchconsole_query_last_16m_export import GoogleSearchConsoleQueryLast16mExport
from exports.screamingfrog_list_crawl_export import ScreamingFrogListCrawlManualExport
from exports.semrush_analytics_organic_positions_rootdomain import SemrushAnalyticsOrganicPositionsRootdomainExport

logger = logging.getLogger(__name__)


class UrlRankingDataProcessor:
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

        semrush_analytics_organic_positions_rootdomain_export = SemrushAnalyticsOrganicPositionsRootdomainExport(self._project)
        self.semrush_analytics_organic_positions_rootdomain_data = semrush_analytics_organic_positions_rootdomain_export.get_data()

        googlesearchconsole_page_query_last_1m_export = GoogleSearchConsolePageQueryLast1mExport(self._project)
        self.googlesearchconsole_page_query_last_1m_data = googlesearchconsole_page_query_last_1m_export.get_data()

        googlesearchconsole_page_query_previous_1m_export = GoogleSearchConsolePageQueryPrevious1mExport(self._project)
        self.googlesearchconsole_page_query_previous_1m_data = googlesearchconsole_page_query_previous_1m_export.get_data()

        googlesearchconsole_page_query_last_1m_previous_year_export = GoogleSearchConsolePageQueryLast1mPreviousYearExport(self._project)
        self.googlesearchconsole_page_query_last_1m_previous_year_data = googlesearchconsole_page_query_last_1m_previous_year_export.get_data()

        googlesearchconsole_page_query_last_16m_export = GoogleSearchConsolePageQueryLast16mExport(self._project)
        self.googlesearchconsole_page_query_last_16m_data = googlesearchconsole_page_query_last_16m_export.get_data()

    def process_data(self):
        # stack key column then map
        df = pd.concat(
            [
                self.screamingfrog_list_crawl_data["Address"],
                self.semrush_analytics_organic_positions_rootdomain_data["URL"],
                self.googlesearchconsole_page_query_last_1m_data["page"],
                self.googlesearchconsole_page_query_previous_1m_data["page"],
                self.googlesearchconsole_page_query_last_1m_previous_year_data["page"],
                self.googlesearchconsole_page_query_last_16m_data["page"],
            ]
        )
        df = pd.DataFrame(df.unique(), columns=["full_address"])
        # Drop rows where the URL has a fragment
        mask = df["full_address"].apply(UrlManager.has_fragment)
        df = df[~mask]

        semrush_analytics_organic_positions_rootdomain_best_by_volume_data = self.semrush_analytics_organic_positions_rootdomain_data.rename(columns=lambda x: x + " (Semrush Best by Volume)" if x != "URL" else x)
        semrush_analytics_organic_positions_rootdomain_data_columns = [
            "Keyword (Semrush Best by Volume)",
            "Position (Semrush Best by Volume)",
            "Previous position (Semrush Best by Volume)",
            "Search Volume (Semrush Best by Volume)",
            "URL",
            "Timestamp (Semrush Best by Volume)",
            "Position Type (Semrush Best by Volume)",
        ]
        df = pd.merge(
            df,
            semrush_analytics_organic_positions_rootdomain_best_by_volume_data[semrush_analytics_organic_positions_rootdomain_data_columns]
            .sort_values(by="Search Volume (Semrush Best by Volume)", ascending=False)
            .drop_duplicates(subset="URL", keep="first"),
            left_on="full_address",
            right_on="URL",
            how="left",
        )
        df.drop("URL", axis=1, inplace=True)

        semrush_analytics_organic_positions_rootdomain_best_by_position_data = self.semrush_analytics_organic_positions_rootdomain_data.rename(columns=lambda x: x + " (Semrush Best by Position)" if x != "URL" else x)
        semrush_analytics_organic_positions_rootdomain_data_columns = [
            "Keyword (Semrush Best by Position)",
            "Position (Semrush Best by Position)",
            "Previous position (Semrush Best by Position)",
            "Search Volume (Semrush Best by Position)",
            "URL",
            "Timestamp (Semrush Best by Position)",
            "Position Type (Semrush Best by Position)",
        ]
        df = pd.merge(
            df,
            semrush_analytics_organic_positions_rootdomain_best_by_position_data[semrush_analytics_organic_positions_rootdomain_data_columns]
            .sort_values(by="Position (Semrush Best by Position)", ascending=True)
            .drop_duplicates(subset="URL", keep="first"),
            left_on="full_address",
            right_on="URL",
            how="left",
        )
        df.drop("URL", axis=1, inplace=True)
        ####################### last_1m_data #############################
        googlesearchconsole_page_query_last_1m_data_best_by_clicks = self.googlesearchconsole_page_query_last_1m_data.rename(columns=lambda x: "gsc_" + x + "_last_1m_best_by_clicks" if x != "page" else x)
        df = pd.merge(
            df,
            googlesearchconsole_page_query_last_1m_data_best_by_clicks.sort_values(by=["gsc_clicks_last_1m_best_by_clicks", "gsc_impressions_last_1m_best_by_clicks"], ascending=[False, False]).drop_duplicates(subset="page", keep="first"),
            left_on="full_address",
            right_on="page",
            how="left",
        )
        df.drop("page", axis=1, inplace=True)

        googlesearchconsole_page_query_last_1m_data_best_by_impressions = self.googlesearchconsole_page_query_last_1m_data.rename(columns=lambda x: "gsc_" + x + "_last_1m_best_by_impressions" if x != "page" else x)
        df = pd.merge(
            df,
            googlesearchconsole_page_query_last_1m_data_best_by_impressions.sort_values(by="gsc_impressions_last_1m_best_by_impressions", ascending=False).drop_duplicates(subset="page", keep="first"),
            left_on="full_address",
            right_on="page",
            how="left",
        )
        df.drop("page", axis=1, inplace=True)

        googlesearchconsole_page_query_last_1m_data_best_by_position = self.googlesearchconsole_page_query_last_1m_data.rename(columns=lambda x: "gsc_" + x + "_last_1m_best_by_position" if x != "page" else x)
        df = pd.merge(
            df,
            googlesearchconsole_page_query_last_1m_data_best_by_position.sort_values(by=["gsc_position_last_1m_best_by_position", "gsc_impressions_last_1m_best_by_position"], ascending=[False, False]).drop_duplicates(
                subset="page", keep="first"
            ),
            left_on="full_address",
            right_on="page",
            how="left",
        )
        df.drop("page", axis=1, inplace=True)
        ####################### end #############################
        ####################### previous_1m_data #############################
        googlesearchconsole_page_query_previous_1m_data_best_by_clicks = self.googlesearchconsole_page_query_previous_1m_data.rename(columns=lambda x: "gsc_" + x + "_previous_1m_best_by_clicks" if x != "page" else x)
        df = pd.merge(
            df,
            googlesearchconsole_page_query_previous_1m_data_best_by_clicks.sort_values(by=["gsc_clicks_previous_1m_best_by_clicks", "gsc_impressions_previous_1m_best_by_clicks"], ascending=[False, False]).drop_duplicates(
                subset="page", keep="first"
            ),
            left_on="full_address",
            right_on="page",
            how="left",
        )
        df.drop("page", axis=1, inplace=True)

        googlesearchconsole_page_query_previous_1m_data_best_by_impressions = self.googlesearchconsole_page_query_previous_1m_data.rename(columns=lambda x: "gsc_" + x + "_previous_1m_best_by_impressions" if x != "page" else x)
        df = pd.merge(
            df,
            googlesearchconsole_page_query_previous_1m_data_best_by_impressions.sort_values(by="gsc_impressions_previous_1m_best_by_impressions", ascending=False).drop_duplicates(subset="page", keep="first"),
            left_on="full_address",
            right_on="page",
            how="left",
        )
        df.drop("page", axis=1, inplace=True)

        googlesearchconsole_page_query_previous_1m_data_best_by_position = self.googlesearchconsole_page_query_previous_1m_data.rename(columns=lambda x: "gsc_" + x + "_previous_1m_best_by_position" if x != "page" else x)
        df = pd.merge(
            df,
            googlesearchconsole_page_query_previous_1m_data_best_by_position.sort_values(by=["gsc_position_previous_1m_best_by_position", "gsc_impressions_previous_1m_best_by_position"], ascending=[False, False]).drop_duplicates(
                subset="page", keep="first"
            ),
            left_on="full_address",
            right_on="page",
            how="left",
        )
        df.drop("page", axis=1, inplace=True)
        ####################### end #############################
        ####################### last_1m_previous_year_data #############################
        googlesearchconsole_page_query_last_1m_previous_year_data_best_by_clicks = self.googlesearchconsole_page_query_last_1m_previous_year_data.rename(
            columns=lambda x: "gsc_" + x + "_last_1m_previous_year_best_by_clicks" if x != "page" else x
        )
        df = pd.merge(
            df,
            googlesearchconsole_page_query_last_1m_previous_year_data_best_by_clicks.sort_values(
                by=["gsc_clicks_last_1m_previous_year_best_by_clicks", "gsc_impressions_last_1m_previous_year_best_by_clicks"], ascending=[False, False]
            ).drop_duplicates(subset="page", keep="first"),
            left_on="full_address",
            right_on="page",
            how="left",
        )
        df.drop("page", axis=1, inplace=True)

        googlesearchconsole_page_query_last_1m_previous_year_data_best_by_impressions = self.googlesearchconsole_page_query_last_1m_previous_year_data.rename(
            columns=lambda x: "gsc_" + x + "_last_1m_previous_year_best_by_impressions" if x != "page" else x
        )
        df = pd.merge(
            df,
            googlesearchconsole_page_query_last_1m_previous_year_data_best_by_impressions.sort_values(by="gsc_impressions_last_1m_previous_year_best_by_impressions", ascending=False).drop_duplicates(subset="page", keep="first"),
            left_on="full_address",
            right_on="page",
            how="left",
        )
        df.drop("page", axis=1, inplace=True)

        googlesearchconsole_page_query_last_1m_previous_year_data_best_by_position = self.googlesearchconsole_page_query_last_1m_previous_year_data.rename(
            columns=lambda x: "gsc_" + x + "_last_1m_previous_year_best_by_position" if x != "page" else x
        )
        df = pd.merge(
            df,
            googlesearchconsole_page_query_last_1m_previous_year_data_best_by_position.sort_values(
                by=["gsc_position_last_1m_previous_year_best_by_position", "gsc_impressions_last_1m_previous_year_best_by_position"], ascending=[False, False]
            ).drop_duplicates(subset="page", keep="first"),
            left_on="full_address",
            right_on="page",
            how="left",
        )
        df.drop("page", axis=1, inplace=True)
        ####################### end #############################
        ####################### last_16m_data #############################
        googlesearchconsole_page_query_last_16m_data_best_by_clicks = self.googlesearchconsole_page_query_last_16m_data.rename(columns=lambda x: "gsc_" + x + "_last_16m_best_by_clicks" if x != "page" else x)
        df = pd.merge(
            df,
            googlesearchconsole_page_query_last_16m_data_best_by_clicks.sort_values(by=["gsc_clicks_last_16m_best_by_clicks", "gsc_impressions_last_16m_best_by_clicks"], ascending=[False, False]).drop_duplicates(
                subset="page", keep="first"
            ),
            left_on="full_address",
            right_on="page",
            how="left",
        )
        df.drop("page", axis=1, inplace=True)

        googlesearchconsole_page_query_last_16m_data_best_by_impressions = self.googlesearchconsole_page_query_last_16m_data.rename(columns=lambda x: "gsc_" + x + "_last_16m_best_by_impressions" if x != "page" else x)
        df = pd.merge(
            df,
            googlesearchconsole_page_query_last_16m_data_best_by_impressions.sort_values(by="gsc_impressions_last_16m_best_by_impressions", ascending=False).drop_duplicates(subset="page", keep="first"),
            left_on="full_address",
            right_on="page",
            how="left",
        )
        df.drop("page", axis=1, inplace=True)

        googlesearchconsole_page_query_last_16m_data_best_by_position = self.googlesearchconsole_page_query_last_16m_data.rename(columns=lambda x: "gsc_" + x + "_last_16m_best_by_position" if x != "page" else x)
        df = pd.merge(
            df,
            googlesearchconsole_page_query_last_16m_data_best_by_position.sort_values(by=["gsc_position_last_16m_best_by_position", "gsc_impressions_last_16m_best_by_position"], ascending=[False, False]).drop_duplicates(
                subset="page", keep="first"
            ),
            left_on="full_address",
            right_on="page",
            how="left",
        )
        df.drop("page", axis=1, inplace=True)
        ####################### end #############################
        self._data = df

    def store_data(self):
        total_rows = len(self._data)
        for index, row in self._data.iterrows():
            row = row.apply(lambda x: None if isna(x) else x)

            url = UrlManager.push_url(row["full_address"], self._website)
            UrlRankingManager.push_ranking(
                url=url,
                website=self._website,
                semrush_keyword_best_by_volume=KeywordManager.push_keyword(row["Keyword (Semrush Best by Volume)"]) if row["Keyword (Semrush Best by Volume)"] else None,
                semrush_current_position_best_by_volume=row["Position (Semrush Best by Volume)"],
                semrush_previous_position_best_by_volume=row["Previous position (Semrush Best by Volume)"],
                semrush_timestamp_best_by_volume=row["Timestamp (Semrush Best by Volume)"],
                semrush_position_type_best_by_volume=row["Position Type (Semrush Best by Volume)"],
                #
                semrush_keyword_best_by_position=KeywordManager.push_keyword(row["Keyword (Semrush Best by Position)"]) if row["Keyword (Semrush Best by Position)"] else None,
                semrush_current_position_best_by_position=row["Position (Semrush Best by Position)"],
                semrush_previous_position_best_by_position=row["Previous position (Semrush Best by Position)"],  #
                semrush_timestamp_best_by_position=row["Timestamp (Semrush Best by Position)"],  #
                semrush_position_type_best_by_position=row["Position Type (Semrush Best by Position)"],  #
                # last_1m
                gsc_query_last_1m_best_by_clicks=KeywordManager.push_keyword(row["gsc_query_last_1m_best_by_clicks"]) if row["gsc_query_last_1m_best_by_clicks"] else None,
                gsc_impressions_last_1m_best_by_clicks=row["gsc_impressions_last_1m_best_by_clicks"],
                gsc_clicks_last_1m_best_by_clicks=row["gsc_clicks_last_1m_best_by_clicks"],
                gsc_ctr_last_1m_best_by_clicks=row["gsc_ctr_last_1m_best_by_clicks"],
                gsc_position_last_1m_best_by_clicks=row["gsc_position_last_1m_best_by_clicks"],
                gsc_query_last_1m_best_by_impressions=KeywordManager.push_keyword(row["gsc_query_last_1m_best_by_impressions"]) if row["gsc_query_last_1m_best_by_impressions"] else None,
                gsc_impressions_last_1m_best_by_impressions=row["gsc_impressions_last_1m_best_by_impressions"],
                gsc_clicks_last_1m_best_by_impressions=row["gsc_clicks_last_1m_best_by_impressions"],
                gsc_ctr_last_1m_best_by_impressions=row["gsc_ctr_last_1m_best_by_impressions"],
                gsc_position_last_1m_best_by_impressions=row["gsc_position_last_1m_best_by_impressions"],
                gsc_query_last_1m_best_by_position=KeywordManager.push_keyword(row["gsc_query_last_1m_best_by_position"]) if row["gsc_query_last_1m_best_by_position"] else None,
                gsc_impressions_last_1m_best_by_position=row["gsc_impressions_last_1m_best_by_position"],
                gsc_clicks_last_1m_best_by_position=row["gsc_clicks_last_1m_best_by_position"],
                gsc_ctr_last_1m_best_by_position=row["gsc_ctr_last_1m_best_by_position"],
                gsc_position_last_1m_best_by_position=row["gsc_position_last_1m_best_by_position"],
                # end
                # previous_1m
                gsc_query_previous_1m_best_by_clicks=KeywordManager.push_keyword(row["gsc_query_previous_1m_best_by_clicks"]) if row["gsc_query_previous_1m_best_by_clicks"] else None,
                gsc_impressions_previous_1m_best_by_clicks=row["gsc_impressions_previous_1m_best_by_clicks"],
                gsc_clicks_previous_1m_best_by_clicks=row["gsc_clicks_previous_1m_best_by_clicks"],
                gsc_ctr_previous_1m_best_by_clicks=row["gsc_ctr_previous_1m_best_by_clicks"],
                gsc_position_previous_1m_best_by_clicks=row["gsc_position_previous_1m_best_by_clicks"],
                gsc_query_previous_1m_best_by_impressions=KeywordManager.push_keyword(row["gsc_query_previous_1m_best_by_impressions"]) if row["gsc_query_previous_1m_best_by_impressions"] else None,
                gsc_impressions_previous_1m_best_by_impressions=row["gsc_impressions_previous_1m_best_by_impressions"],
                gsc_clicks_previous_1m_best_by_impressions=row["gsc_clicks_previous_1m_best_by_impressions"],
                gsc_ctr_previous_1m_best_by_impressions=row["gsc_ctr_previous_1m_best_by_impressions"],
                gsc_position_previous_1m_best_by_impressions=row["gsc_position_previous_1m_best_by_impressions"],
                gsc_query_previous_1m_best_by_position=KeywordManager.push_keyword(row["gsc_query_previous_1m_best_by_position"]) if row["gsc_query_previous_1m_best_by_position"] else None,
                gsc_impressions_previous_1m_best_by_position=row["gsc_impressions_previous_1m_best_by_position"],
                gsc_clicks_previous_1m_best_by_position=row["gsc_clicks_previous_1m_best_by_position"],
                gsc_ctr_previous_1m_best_by_position=row["gsc_ctr_previous_1m_best_by_position"],
                gsc_position_previous_1m_best_by_position=row["gsc_position_previous_1m_best_by_position"],
                # end
                # last_1m_previous_year
                gsc_query_last_1m_previous_year_best_by_clicks=KeywordManager.push_keyword(row["gsc_query_last_1m_previous_year_best_by_clicks"]) if row["gsc_query_last_1m_previous_year_best_by_clicks"] else None,
                gsc_impressions_last_1m_previous_year_best_by_clicks=row["gsc_impressions_last_1m_previous_year_best_by_clicks"],
                gsc_clicks_last_1m_previous_year_best_by_clicks=row["gsc_clicks_last_1m_previous_year_best_by_clicks"],
                gsc_ctr_last_1m_previous_year_best_by_clicks=row["gsc_ctr_last_1m_previous_year_best_by_clicks"],
                gsc_position_last_1m_previous_year_best_by_clicks=row["gsc_position_last_1m_previous_year_best_by_clicks"],
                gsc_query_last_1m_previous_year_best_by_impressions=KeywordManager.push_keyword(row["gsc_query_last_1m_previous_year_best_by_impressions"]) if row["gsc_query_last_1m_previous_year_best_by_impressions"] else None,
                gsc_impressions_last_1m_previous_year_best_by_impressions=row["gsc_impressions_last_1m_previous_year_best_by_impressions"],
                gsc_clicks_last_1m_previous_year_best_by_impressions=row["gsc_clicks_last_1m_previous_year_best_by_impressions"],
                gsc_ctr_last_1m_previous_year_best_by_impressions=row["gsc_ctr_last_1m_previous_year_best_by_impressions"],
                gsc_position_last_1m_previous_year_best_by_impressions=row["gsc_position_last_1m_previous_year_best_by_impressions"],
                gsc_query_last_1m_previous_year_best_by_position=KeywordManager.push_keyword(row["gsc_query_last_1m_previous_year_best_by_position"]) if row["gsc_query_last_1m_previous_year_best_by_position"] else None,
                gsc_impressions_last_1m_previous_year_best_by_position=row["gsc_impressions_last_1m_previous_year_best_by_position"],
                gsc_clicks_last_1m_previous_year_best_by_position=row["gsc_clicks_last_1m_previous_year_best_by_position"],
                gsc_ctr_last_1m_previous_year_best_by_position=row["gsc_ctr_last_1m_previous_year_best_by_position"],
                gsc_position_last_1m_previous_year_best_by_position=row["gsc_position_last_1m_previous_year_best_by_position"],
                # end
                # last_16m
                gsc_query_last_16m_best_by_clicks=KeywordManager.push_keyword(row["gsc_query_last_16m_best_by_clicks"]) if row["gsc_query_last_16m_best_by_clicks"] else None,
                gsc_impressions_last_16m_best_by_clicks=row["gsc_impressions_last_16m_best_by_clicks"],
                gsc_clicks_last_16m_best_by_clicks=row["gsc_clicks_last_16m_best_by_clicks"],
                gsc_ctr_last_16m_best_by_clicks=row["gsc_ctr_last_16m_best_by_clicks"],
                gsc_position_last_16m_best_by_clicks=row["gsc_position_last_16m_best_by_clicks"],
                gsc_query_last_16m_best_by_impressions=KeywordManager.push_keyword(row["gsc_query_last_16m_best_by_impressions"]) if row["gsc_query_last_16m_best_by_impressions"] else None,
                gsc_impressions_last_16m_best_by_impressions=row["gsc_impressions_last_16m_best_by_impressions"],
                gsc_clicks_last_16m_best_by_impressions=row["gsc_clicks_last_16m_best_by_impressions"],
                gsc_ctr_last_16m_best_by_impressions=row["gsc_ctr_last_16m_best_by_impressions"],
                gsc_position_last_16m_best_by_impressions=row["gsc_position_last_16m_best_by_impressions"],
                gsc_query_last_16m_best_by_position=KeywordManager.push_keyword(row["gsc_query_last_16m_best_by_position"]) if row["gsc_query_last_16m_best_by_position"] else None,
                gsc_impressions_last_16m_best_by_position=row["gsc_impressions_last_16m_best_by_position"],
                gsc_clicks_last_16m_best_by_position=row["gsc_clicks_last_16m_best_by_position"],
                gsc_ctr_last_16m_best_by_position=row["gsc_ctr_last_16m_best_by_position"],
                gsc_position_last_16m_best_by_position=row["gsc_position_last_16m_best_by_position"],
            )
            if index % 100 == 0 or index == total_rows:
                logger.info(f"UrlRankingManager: Processing Ranking Data: Row {index} of {total_rows} ({(index / total_rows) * 100:.2f}% complete)")

        logger.info("Data successfully processed using UrlRankingManager.")
