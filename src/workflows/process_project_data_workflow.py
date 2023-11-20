import pandas as pd

from core.managers.url_manager import UrlManager
from exports.googlesearchconsole_last_16m_page_export import GoogleSearchConsoleLast16mPageExport
from exports.screamingfrog_sitemap_crawl_export import ScreamingFrogSitemapCrawlExport
from exports.screamingfrog_spider_crawl_export import ScreamingFrogSpiderCrawlExport
from exports.sitebulb_url_internal_export import SitebulbUrlInternalExport
from core.managers.project_manager import ProjectManager


class ProcessProjectDataWorkflow:
    def __init__(self, project):
        self.project_id = project.id

    def execute(self):
        project = ProjectManager.get_project(project_id=self.project_id)
        master_url_data = pd.DataFrame(
            columns=[
                "url",
                "status_code",
            ]
        )

        def merge_data(master_data, new_data, master_data_key, new_data_key, master_data_column, new_data_column):
            new_data = new_data[[new_data_key, new_data_column]].drop_duplicates()
            new_data[new_data_key] = new_data[new_data_key].astype(str)

            # Identify and append new key values
            new_keys = new_data[~new_data[new_data_key].isin(master_data[master_data_key])][new_data_key]
            master_data = pd.concat([master_data, new_keys.to_frame(name=master_data_key)], ignore_index=True)

            # Perform a left join
            merged_data = master_data.merge(new_data, left_on=master_data_key, right_on=new_data_key, how="left", suffixes=("", "_new"))

            # Update only missing values in master_data_column
            master_data[master_data_column] = master_data[master_data_column].combine_first(merged_data[new_data_column])

            return master_data

        # Example usage of merge_data for different data sources
        screamingfrog_sitemap_crawl_export = ScreamingFrogSitemapCrawlExport(project)
        screamingfrog_sitemap_crawl_data = screamingfrog_sitemap_crawl_export.get_data()
        master_url_data = merge_data(master_url_data, screamingfrog_sitemap_crawl_data, "url", "Address", "status_code", "Status Code")

        screamingfrog_spider_crawl_export = ScreamingFrogSpiderCrawlExport(project)
        screamingfrog_spider_crawl_data = screamingfrog_spider_crawl_export.get_data()
        master_url_data = merge_data(master_url_data, screamingfrog_spider_crawl_data, "url", "Address", "status_code", "Status Code")

        sitebulb_url_internal_export = SitebulbUrlInternalExport(project)
        sitebulb_url_internal_data = sitebulb_url_internal_export.get_data()
        master_url_data = merge_data(master_url_data, sitebulb_url_internal_data, "url", "URL", "status_code", "HTTP Status Code")

        a = 1

    # screamingfrog_sitemap_crawl_export = ScreamingFrogSitemapCrawlExport(project)
    # screamingfrog_sitemap_crawl_data = screamingfrog_sitemap_crawl_export.get_data()
    # master_url_data = process_and_merge(screamingfrog_sitemap_crawl_data, "Address", "Status Code")
    #
    # screamingfrog_spider_crawl_export = ScreamingFrogSpiderCrawlExport(project)
    # screamingfrog_spider_crawl_data = screamingfrog_spider_crawl_export.get_data()
    #
    # sitebulb_url_internal_export = SitebulbUrlInternalExport(project)
    # sitebulb_url_internal_data = sitebulb_url_internal_export.get_data()
    #
    # googlesearchconsole_last_16m_page_export = GoogleSearchConsoleLast16mPageExport(project)
    # googlesearchconsole_last_16m_page_data = googlesearchconsole_last_16m_page_export.get_data()
    #
    # sitemap_urls = screamingfrog_sitemap_crawl_data["Address"]
    # spider_urls = screamingfrog_spider_crawl_data["Address"]
    # sitebulb_urls = sitebulb_url_internal_data["URL"]
    # gsc_urls = googlesearchconsole_last_16m_page_data["page"]
    #
    # # Combine all URL data into a single Series
    # all_urls = pd.concat([sitemap_urls, spider_urls, sitebulb_urls, gsc_urls])
    #
    # # Extract unique URLs
    # unique_urls = pd.DataFrame(pd.Series(all_urls.unique()), columns=["url"])
    #
    # # Apply the UrlManager.remove_url_fragment function to each URL
    # unique_urls["url"] = unique_urls["url"].apply(UrlManager.remove_url_fragment)
    #
    # # Deduplicate the DataFrame
    # master_url_data = unique_urls.drop_duplicates()
    #
    # # Prepare the DataFrames for merging
    # master_url_data.reset_index(inplace=True)  # Reset index if the URL is the index
    # screamingfrog_sitemap_crawl_data["Address"] = screamingfrog_sitemap_crawl_data["Address"].astype(str)
    # screamingfrog_spider_crawl_data["Address"] = screamingfrog_spider_crawl_data["Address"].astype(str)
    # sitebulb_url_internal_data["URL"] = sitebulb_url_internal_data["URL"].astype(str)
    #
    # # Merge the status_code columns
    # master_url_data = master_url_data.merge(
    #     screamingfrog_sitemap_crawl_data[["Address", "Status Code"]],
    #     left_on="url",
    #     right_on="Address",
    #     how="left",
    # )
    # master_url_data = master_url_data.merge(
    #     screamingfrog_spider_crawl_data[["Address", "Status Code"]],
    #     left_on="url",
    #     right_on="Address",
    #     how="left",
    #     suffixes=("_sitemap", "_spider"),
    # )
    # master_url_data = master_url_data.merge(
    #     sitebulb_url_internal_data[["URL", "HTTP Status Code"]],
    #     left_on="url",
    #     right_on="URL",
    #     how="left",
    # )
    #
    # # Consolidate status_code columns
    # # Priority: sitemap > spider > sitebulb
    # master_url_data["final_status_code"] = (
    #     master_url_data["Status Code_sitemap"]
    #     .fillna(master_url_data["Status Code_spider"])
    #     .fillna(master_url_data["HTTP Status Code"])
    # )
    #
    # # Drop temporary columns
    # master_url_data.drop(
    #     [
    #         "Address_sitemap",
    #         "Address_spider",
    #         "URL",
    #         "HTTP Status Code",
    #         "Status Code_sitemap",
    #         "Status Code_spider",
    #         "index",
    #     ],
    #     axis=1,
    #     inplace=True,
    # )
    #
    # # Rename final_status_code to status_code
    # master_url_data.rename(columns={"final_status_code": "status_code"}, inplace=True)
    #
    # a = 1
