import logging

import numpy as np
import pandas as pd

from core.models.project import ProjectManager
from core.models.url import UrlManager
from core.models.website import WebsiteManager
from exports.googlesearchconsole_page_last_16m_export import GoogleSearchConsolePageLast16mExport
from exports.screamingfrog_list_crawl_export import ScreamingFrogListCrawlExport
from exports.screamingfrog_sitemap_crawl_export import ScreamingFrogSitemapCrawlManualExport
from exports.screamingfrog_spider_crawl_export import ScreamingFrogSpiderCrawlManualExport
from exports.sitebulb_spider_crawl_url_internal_export import SitebulbSpiderCrawlUrlInternalExport
from services.dataframe_service import DataframeService

logger = logging.getLogger(__name__)


class UpdateProjectUrlsWorkflow:
    def __init__(self, project):
        self.project_id = project.id

    def execute(self):
        project = ProjectManager.get_project_by_id(project_id=self.project_id)
        website = WebsiteManager.get_website_by_project(project=project)
        master_url_data = pd.DataFrame(
            columns=[
                "url",
                "status_code",
                "redirect_url",
            ]
        )

        # build URL list
        screamingfrog_sitemap_crawl_export = ScreamingFrogSitemapCrawlManualExport(project)
        screamingfrog_sitemap_crawl_data = screamingfrog_sitemap_crawl_export.get_data()
        # clean urls
        screamingfrog_sitemap_crawl_data = screamingfrog_sitemap_crawl_data[~screamingfrog_sitemap_crawl_data["Content Type"].str.contains("application/xml")]  # remove sitemap urls
        screamingfrog_sitemap_crawl_data = screamingfrog_sitemap_crawl_data[~screamingfrog_sitemap_crawl_data["Content Type"].str.contains("image/jpeg")]  # remove jpg images
        screamingfrog_sitemap_crawl_data = screamingfrog_sitemap_crawl_data[~screamingfrog_sitemap_crawl_data["Content Type"].str.contains("image/png")]  # remove png images
        screamingfrog_sitemap_crawl_data = screamingfrog_sitemap_crawl_data[~screamingfrog_sitemap_crawl_data["Content Type"].str.contains("image/webp")]  # remove webp images

        # merge clean urls in master data
        master_url_data = DataframeService.merge_keys(master_url_data, screamingfrog_sitemap_crawl_data, "url", "Address")

        screamingfrog_spider_crawl_export = ScreamingFrogSpiderCrawlManualExport(project)
        screamingfrog_spider_crawl_data = screamingfrog_spider_crawl_export.get_data()
        # clean urls

        # merge clean urls in master data
        master_url_data = DataframeService.merge_keys(master_url_data, screamingfrog_spider_crawl_data, "url", "Address")

        sitebulb_url_internal_export = SitebulbSpiderCrawlUrlInternalExport(project)
        sitebulb_url_internal_data = sitebulb_url_internal_export.get_data()
        # clean urls

        # merge clean urls in master data
        master_url_data = DataframeService.merge_keys(master_url_data, sitebulb_url_internal_data, "url", "URL")

        googlesearchconsole_last_16m_page_export = GoogleSearchConsolePageLast16mExport(project)
        googlesearchconsole_last_16m_page_data = googlesearchconsole_last_16m_page_export.get_data()
        # clean urls
        googlesearchconsole_last_16m_page_data_clean = googlesearchconsole_last_16m_page_data[~googlesearchconsole_last_16m_page_data["page"].str.contains("#")]

        # merge clean urls in master data
        master_url_data = DataframeService.merge_keys(master_url_data, googlesearchconsole_last_16m_page_data_clean, "url", "page")

        # update status_code column
        master_url_data = DataframeService.merge_data(master_url_data, screamingfrog_sitemap_crawl_data, "url", "Address", "status_code", "Status Code")
        master_url_data = DataframeService.merge_data(master_url_data, screamingfrog_spider_crawl_data, "url", "Address", "status_code", "Status Code")
        master_url_data = DataframeService.merge_data(master_url_data, sitebulb_url_internal_data, "url", "URL", "status_code", "HTTP Status Code")

        # update redirect_url column
        master_url_data = DataframeService.merge_data(master_url_data, screamingfrog_sitemap_crawl_data, "url", "Address", "redirect_url", "Redirect URL")
        master_url_data = DataframeService.merge_data(master_url_data, screamingfrog_spider_crawl_data, "url", "Address", "redirect_url", "Redirect URL")

        sitebulb_url_internal_data["Redirect URL"] = sitebulb_url_internal_data["Redirect URL"].replace("--", np.nan)
        master_url_data = DataframeService.merge_data(master_url_data, sitebulb_url_internal_data, "url", "URL", "redirect_url", "Redirect URL")

        # Filter and print URLs where status_code is NaN
        urls_with_nan_status_code = master_url_data[master_url_data["status_code"].isna()]["url"]
        urls_with_nan_status_code.to_clipboard(index=False, header=False)

        screamingfrog_list_crawl_export = ScreamingFrogListCrawlExport(project)
        screamingfrog_list_crawl_export.run()
        print(urls_with_nan_status_code)

        screamingfrog_list_crawl_export = ScreamingFrogListCrawlExport(project)
        screamingfrog_list_crawl_data = screamingfrog_list_crawl_export.get_data()
        master_url_data = DataframeService.merge_data(master_url_data, screamingfrog_list_crawl_data, "url", "Address", "status_code", "Status Code")
        master_url_data = DataframeService.merge_data(master_url_data, screamingfrog_list_crawl_data, "url", "Address", "redirect_url", "Redirect URL")

        # Iterate through each row in the DataFrame
        for index, row in master_url_data.iterrows():
            try:
                # Extract data from the row
                url = row["url"]
                status_code = row["status_code"]
                redirect_url = row["redirect_url"]

                # Use UrlManager to update or create the URL object
                UrlManager.update_url(
                    full_address=url,
                    status_code=status_code,
                    redirect_url=redirect_url,
                    website=website,
                )

            except Exception as e:
                # Handle exceptions (optional)
                print(f"Error processing URL {url}: {e}")
