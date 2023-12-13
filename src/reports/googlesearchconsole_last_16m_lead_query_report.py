import logging
import os
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from core.models.url import UrlManager
from core.models.website import WebsiteManager
from core.models import Project
from exports.googlesearchconsole_last_16m_page_query_export import GoogleSearchConsoleLast16mPageQueryExport
from reports.base_report_generator import BaseReportGenerator
from services.dataframe_service import DataframeService


class GoogleSearchConsoleLast16mLeadQueryReport(BaseReportGenerator):
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

        export = GoogleSearchConsoleLast16mPageQueryExport(self.project)
        export_df = export.get_data()
        export_df = export_df[~export_df["page"].str.contains("#")]

        joined_df = export_df.merge(urls_df, left_on="page", right_on="full_address", how="inner")

        # Group by 'redirect_url' and 'query'
        grouped = joined_df.groupby(["redirect_url", "query"])

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
            }
            aggregated_results.append(aggregated_data)

            print(f"Merged data for {redirect_url} - {query}")

        # Convert the list to a DataFrame
        aggregated_results_df = pd.DataFrame(aggregated_results)

        # Merge the aggregated results back with the original DataFrame
        final_df = joined_df.merge(aggregated_results_df, on=["redirect_url", "query"], how="left")

        # Group the final_df by 'page'
        grouped_final = final_df.groupby("page")

        # Preallocate a list for the final results
        final_results = []

        # Iterate over each group
        for page, group in grouped_final:
            # Initialize default row in case of all NaN values
            default_row = {"query": None, "clicks_sum": 0, "impressions_sum": 0, "ctr_mean": 0, "position_mean": 0}

            # Find rows with max/min criteria, handling NaN values
            max_clicks_row = group.loc[group["clicks_sum"].dropna().idxmax()] if not group["clicks_sum"].isna().all() else default_row
            max_impressions_row = group.loc[group["impressions_sum"].dropna().idxmax()] if not group["impressions_sum"].isna().all() else default_row
            max_ctr_row = group.loc[group["ctr_mean"].dropna().idxmax()] if not group["ctr_mean"].isna().all() else default_row
            min_position_row = group.loc[group["position_mean"].dropna().idxmin()] if not group["position_mean"].isna().all() else default_row

            # Compile the data for this page
            page_data = {
                "url": page,
                "max_clicks_query": max_clicks_row["query"],
                "max_clicks_clicks": max_clicks_row["clicks_sum"],
                "max_clicks_impressions": max_clicks_row["impressions_sum"],
                "max_clicks_ctr": max_clicks_row["ctr_mean"],
                "max_clicks_position": max_clicks_row["position_mean"],
                "max_impressions_query": max_impressions_row["query"],
                "max_impressions_clicks": max_impressions_row["clicks_sum"],
                "max_impressions_impressions": max_impressions_row["impressions_sum"],
                "max_impressions_ctr": max_impressions_row["ctr_mean"],
                "max_impressions_position": max_impressions_row["position_mean"],
                "max_ctr_query": max_ctr_row["query"],
                "max_ctr_clicks": max_ctr_row["clicks_sum"],
                "max_ctr_impressions": max_ctr_row["impressions_sum"],
                "max_ctr_ctr": max_ctr_row["ctr_mean"],
                "max_ctr_position": max_ctr_row["position_mean"],
                "min_position_query": min_position_row["query"],
                "min_position_clicks": min_position_row["clicks_sum"],
                "min_position_impressions": min_position_row["impressions_sum"],
                "min_position_ctr": min_position_row["ctr_mean"],
                "min_position_position": min_position_row["position_mean"],
            }

            final_results.append(page_data)

        # Convert the list to a DataFrame
        final_results_df = pd.DataFrame(final_results)

        # Save the final DataFrame to CSV
        final_report_path = os.path.join(self.project.data_folder, "_reports", "google_search_console_last_16m_lead_query_report.csv")
        final_results_df.to_csv(final_report_path, index=False)

        print(f"Report saved to {final_report_path}")
