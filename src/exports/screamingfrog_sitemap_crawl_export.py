from core.managers.url_manager import UrlManager
from core.managers.website_manager import WebsiteManager
from exports.base_export_manager import BaseExportManager
import logging

logger = logging.getLogger(__name__)
EXPORT_SUBFOLDER = "screamingfrog_sitemap_crawl_export"


class ScreamingFrogSitemapCrawlExport(BaseExportManager):
    def __init__(self, project):
        super().__init__(project, EXPORT_SUBFOLDER)

    def perform_pre_export_action(self):
        """
        Provides instructions for Screaming Frog sitemap crawl export.
        """
        print(f"Please export the Screaming Frog sitemap crawl data as CSV. Verify for 429 error codes.")
        print(f"Place the exported file(s) in the following directory: {self.export_path}")

    def perform_export(self):
        """
        Implement the actual export logic here.
        """
        # Export logic or automated steps specific to Screaming Frog
        pass

    def perform_post_export_action(self):
        """
        Any post-export actions.
        """
        input("Press ENTER to continue after placing the exported files.")
        df = self.get_data()
        # clean urls
        df = df[~df["Address"].str.contains("#")]  # remove fragments
        df = df[~df["Content Type"].str.contains("application/xml")]  # remove sitemap urls
        df = df[~df["Content Type"].str.contains("image/jpeg")]  # remove jpg images
        df = df[~df["Content Type"].str.contains("image/png")]  # remove png images
        df = df[~df["Content Type"].str.contains("image/webp")]  # remove webp images
        # process urls
        all_urls = df["Address"].unique()
        website = WebsiteManager.get_website_by_project(self.project)
        for url in all_urls:
            UrlManager.push_url(full_address=url, website=website)
