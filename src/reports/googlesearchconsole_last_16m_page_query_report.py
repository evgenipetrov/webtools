import logging
import os
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from core.managers.url_manager import UrlManager
from core.managers.website_manager import WebsiteManager
from core.models import Project
from exports.googlesearchconsole_last_16m_page_query_export import GoogleSearchConsoleLast16mPageQueryExport
from reports.base_report_generator import BaseReportGenerator
from services.dataframe_service import DataframeService


class GoogleSearchConsoleLast16mPageQueryReport(BaseReportGenerator):
    """
    Report generator for Google Search Console Last 16 Months Page Report.
    """

    def __init__(self, project: Project) -> None:
        self.project = project

    # def run(self):
    #     website = WebsiteManager.get_website_by_project(self.project)
    #     urls = UrlManager.get_urls_by_website(website)
    #     urls_df = DataframeService.queryset_to_dataframe(urls)
    #     urls_df.loc[urls_df["status_code"] == 200, "redirect_url"] = urls_df.loc[urls_df["status_code"] == 200, "full_address"]
    #
    #     export = GoogleSearchConsoleLast16mPageQueryExport(self.project)
    #     export_df = export.get_data()
    #     export_df = export_df[~export_df["page"].str.contains("#")]
    #
    #     joined_df = export_df.merge(urls_df, left_on="page", right_on="full_address", how="inner")
    #
    #     grouped_data = joined_df.groupby(["redirect_url", "query"]).agg({"clicks": "sum", "impressions": "sum", "ctr": "mean", "position": "mean"})
    #     joined_df = joined_df.merge(grouped_data, on="redirect_url", how="left", suffixes=("", "_grouped"))
    #
    #     report_df = joined_df[["page", "query", "clicks_grouped", "impressions_grouped", "ctr_grouped", "position_grouped"]]
    #
    #     report_path = os.path.join(self.project.data_folder, "_reports", "google_search_console_last_16m_page_query_report.csv")
    #     report_df.to_csv(report_path)
    def run(self):
        website = WebsiteManager.get_website_by_project(self.project)
        urls = UrlManager.get_urls_by_website(website)
        urls_df = DataframeService.queryset_to_dataframe(urls)
        urls_df.loc[urls_df["status_code"] == 200, "redirect_url"] = urls_df.loc[urls_df["status_code"] == 200, "full_address"]

        export = GoogleSearchConsoleLast16mPageQueryExport(self.project)
        export_df = export.get_data()
        export_df = export_df[~export_df["page"].str.contains("#")]

        joined_df = export_df.merge(urls_df, left_on="page", right_on="full_address", how="inner")

        # Initialize an empty DataFrame for the aggregated results
        aggregated_results = pd.DataFrame()

        # Iterate over each unique combination of redirect_url and query
        for redirect_url, query in joined_df[["redirect_url", "query"]].drop_duplicates().itertuples(index=False):
            # Filter the DataFrame for the current combination
            temp_df = joined_df[(joined_df["redirect_url"] == redirect_url) & (joined_df["query"] == query)]

            # Perform the aggregations
            aggregated_data = {
                "redirect_url": redirect_url,
                "query": query,
                "clicks_sum": temp_df["clicks"].sum(),
                "impressions_sum": temp_df["impressions"].sum(),
                "ctr_mean": temp_df["ctr"].mean(),
                "position_mean": temp_df["position"].mean(),
            }

            # Append the aggregated data to the results DataFrame
            aggregated_results = pd.concat([aggregated_results, pd.DataFrame([aggregated_data])], ignore_index=True)

            print(f"Merged data for {redirect_url} - {query}")

        # Merge the aggregated results back with the original DataFrame
        final_df = joined_df.merge(aggregated_results, on=["redirect_url", "query"], how="left")

        # Selecting the required columns for the report
        report_df = final_df[["page", "query", "clicks_sum", "impressions_sum", "ctr_mean", "position_mean"]]

        report_path = os.path.join(self.project.data_folder, "_reports", "google_search_console_last_16m_page_query_report.csv")
        report_df.to_csv(report_path, index=False)

    def run2(self):
        website = WebsiteManager.get_website_by_project(self.project)
        urls = UrlManager.get_urls_by_website(website)
        urls_df = DataframeService.queryset_to_dataframe(urls)
        urls_df.loc[urls_df["status_code"] == 200, "redirect_url"] = urls_df.loc[urls_df["status_code"] == 200, "full_address"]

        export = GoogleSearchConsoleLast16mPageQueryExport(self.project)
        export_df = export.get_data()
        export_df = export_df[~export_df["page"].str.contains("#")]

        joined_df = export_df.merge(urls_df, left_on="page", right_on="full_address", how="inner")

        # Group by 'redirect_url' and 'query'
        grouped = joined_df.groupby(["redirect_url", "query"])

        # Calculate the sum of query counts for each redirect_url
        query_count_per_url = joined_df.groupby("redirect_url")["query"].count()

        # Preallocate a list for aggregated results
        aggregated_results = []

        # Iterate over each group
        for (redirect_url, query), group in grouped:
            # Perform the aggregations directly
            aggregated_data = {
                "redirect_url": redirect_url,
                "query": query,
                "clicks_sum": group["clicks"].sum(),
                "impressions_sum": group["impressions"].sum(),
                "ctr_mean": group["ctr"].mean().round(2),
                "position_mean": group["position"].mean().round(2),
                "query_count_sum": query_count_per_url[redirect_url],  # Query count sum for the redirect_url
            }
            aggregated_results.append(aggregated_data)

            print(f"Merged data for {redirect_url} - {query}")

        # Convert the list to a DataFrame
        aggregated_results_df = pd.DataFrame(aggregated_results)

        # Merge the aggregated results back with the original DataFrame
        final_df = joined_df.merge(aggregated_results_df, on=["redirect_url", "query"], how="left")

        # Selecting the required columns for the report
        report_df = final_df[["page", "query", "clicks_sum", "impressions_sum", "ctr_mean", "position_mean", "query_count_sum"]]

        report_path = os.path.join(self.project.data_folder, "_reports", "google_search_console_last_16m_page_query_report.csv")
        report_df.to_csv(report_path, index=False)