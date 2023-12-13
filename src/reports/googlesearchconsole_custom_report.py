import logging
import os
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from core.models.url import UrlManager
from core.models.website import WebsiteManager
from core.models import Project
from exports.googlesearchconsole_custom_export import GoogleSearchConsoleCustomExport
from exports.googlesearchconsole_last_16m_page_query_export import GoogleSearchConsoleLast16mPageQueryExport
from reports.base_report_generator import BaseReportGenerator
from services.dataframe_service import DataframeService


class GoogleSearchConsoleCustomReport(BaseReportGenerator):
    """
    Report generator for Google Search Console Last 16 Months Page Report.
    """

    def __init__(self, project: Project) -> None:
        self.project = project

    def run2(self):
        website = WebsiteManager.get_website_by_project(self.project)
        urls = UrlManager.get_urls_by_website(website)
        urls_df = DataframeService.queryset_to_dataframe(urls)
        urls_df.loc[urls_df["status_code"] == 200, "redirect_url"] = urls_df.loc[urls_df["status_code"] == 200, "full_address"]

        export = GoogleSearchConsoleCustomExport(self.project)
        export_df = export.get_data()
        export_df = export_df[~export_df["page"].str.contains("#")]

        joined_df = export_df.merge(urls_df, left_on="page", right_on="full_address", how="inner")

        # Group by 'redirect_url'
        grouped = joined_df.groupby("redirect_url")

        # Preallocate a list for aggregated results
        aggregated_results = []

        # Iterate over each group
        for redirect_url, group in grouped:
            # Perform the aggregations directly
            aggregated_data = {
                "redirect_url": redirect_url,
                "clicks_sum": group["clicks"].sum(),
                "impressions_sum": group["impressions"].sum(),
                "ctr_mean": group["ctr"].mean().round(2),
                "position_mean": group["position"].mean().round(2),
            }
            aggregated_results.append(aggregated_data)

            print(f"Merged data for {redirect_url}")

        # Convert the list to a DataFrame
        aggregated_results_df = pd.DataFrame(aggregated_results)

        # Merge the aggregated results back with the original DataFrame
        final_df = joined_df.merge(aggregated_results_df, on="redirect_url", how="left")

        # Selecting the required columns for the report
        report_df = final_df[["page", "clicks_sum", "impressions_sum", "ctr_mean", "position_mean"]]

        report_path = os.path.join(self.project.data_folder, "_reports", "google_search_console_custom_report.csv")
        report_df.to_csv(report_path, index=False)
