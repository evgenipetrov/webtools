import logging
import os
from datetime import datetime, timedelta
from typing import Any

from core.managers.url_manager import UrlManager
from core.managers.website_manager import WebsiteManager
from core.models import Project
from exports.googlesearchconsole_last_16m_page_export import GoogleSearchConsoleLast16mPageExport
from reports.base_report_generator import BaseReportGenerator
from services.dataframe_service import DataframeService


class GoogleSearchConsoleLast16mPageReport(BaseReportGenerator):
    """
    Report generator for Google Search Console Last 16 Months Page Report.
    """

    def __init__(self, project: Project) -> None:
        self.project = project

        # def run(self) -> None:
        #     website = WebsiteManager.get_website_by_project(self.project)
        #     urls = UrlManager.get_urls_by_website(website)
        #     urls_df = DataframeService.queryset_to_dataframe(urls)
        #     urls_df.loc[urls_df["status_code"] == 200, "redirect_url"] = urls_df.loc[urls_df["status_code"] == 200, "full_address"]
        #
        #     export = GoogleSearchConsoleLast16mPageExport(self.project)
        #     export_df = export.get_data()
        #     export_df = export_df[~export_df["page"].str.contains("#")]
        #
        #     # Step 1: Merge export_df with urls_df
        #     joined_df = export_df.merge(urls_df, left_on="page", right_on="full_address", how="inner")
        #
        #     # Step 2: Perform the required group-by operations
        #     grouped_df = joined_df.groupby("redirect_url").agg({"clicks": "sum", "impressions": "sum", "ctr": "mean", "position": "mean"}).reset_index()
        #
        #     # Step 3: Merge the aggregated data back into export_df
        #     final_df = joined_df.merge(grouped_df, left_on="page", right_on="redirect_url", how="left", suffixes=("", "_merged"))

    def run(self):
        website = WebsiteManager.get_website_by_project(self.project)
        urls = UrlManager.get_urls_by_website(website)
        urls_df = DataframeService.queryset_to_dataframe(urls)
        urls_df.loc[urls_df["status_code"] == 200, "redirect_url"] = urls_df.loc[urls_df["status_code"] == 200, "full_address"]

        export = GoogleSearchConsoleLast16mPageExport(self.project)
        export_df = export.get_data()
        export_df = export_df[~export_df["page"].str.contains("#")]

        joined_df = export_df.merge(urls_df, left_on="page", right_on="full_address", how="inner")

        grouped_data = joined_df.groupby("redirect_url").agg({"clicks": "sum", "impressions": "sum", "ctr": "mean", "position": "mean"})
        joined_df = joined_df.merge(grouped_data, on="redirect_url", how="left", suffixes=("", "_grouped"))

        report_df = joined_df[["page", "clicks_grouped", "impressions_grouped", "ctr_grouped", "position_grouped"]]

        report_path = os.path.join(self.project.data_folder, "_reports", "google_search_console_last_16m_page_report.csv")
        report_df.to_csv(report_path)
