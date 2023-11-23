from core.managers.url_manager import UrlManager
from core.managers.website_manager import WebsiteManager
from exports.base_export_manager import BaseExportManager
import logging

logger = logging.getLogger(__name__)
EXPORT_SUBFOLDER = "semrush_analytics_backlinks_rootdomain"


class SemrushAnalyticsBacklinksRootdomainExport(BaseExportManager):
    def __init__(self, project):
        super().__init__(project, EXPORT_SUBFOLDER)

    def perform_pre_export_action(self):
        """
        Provides instructions for Semrush backlinks export for root domain.
        """
        domain = UrlManager.get_root_url(self.project.base_url)
        url = f"https://www.semrush.com/analytics/backlinks/backlinks/?q={domain}&searchType=domain"
        print(f"Open the following URL and perform export.")
        print(url)
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
        df = df[~df["Target url"].str.contains("#")]  # remove fragments
        # process urls
        all_urls = df["Target url"].unique()
        website = WebsiteManager.get_website_by_project(self.project)
        for url in all_urls:
            UrlManager.push_url(full_address=url, website=website)
