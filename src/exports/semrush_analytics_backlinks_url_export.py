import logging

import pandas as pd

from base_models.base_export_manager import BaseExportManager
from core.models.url import UrlManager
from core.models.website import WebsiteManager

logger = logging.getLogger(__name__)
EXPORT_SUBFOLDER = "semrush_analytics_backlinks_url"


class SemrushAnalyticsBacklinksUrlExport(BaseExportManager):
    def __init__(self, project):
        super().__init__(project, EXPORT_SUBFOLDER)

    def perform_pre_export_action(self):
        """
        Provides instructions for Semrush backlinks export for urls.
        """
        # Check if the user wants to proceed
        if not self.force and not self.confirm_export(EXPORT_SUBFOLDER):
            print("Export process aborted.")
            return  # Stop the method if user does not confirm

        print(f"Open the following URL and perform export.")
        print(f"Place the exported file(s) in the following directory: {self.export_path}")
        website = WebsiteManager.get_website_by_project(self.project)
        urls = UrlManager.get_urls_by_website(website)

        # Collect URLs in a DataFrame
        addresses = [f"https://www.semrush.com/analytics/backlinks/backlinks/?q={url.full_address}&searchType=url" for url in urls]
        df = pd.DataFrame(addresses, columns=["URL"])

        # Printing and copying URLs to clipboard
        # print(df.to_string(index=False))
        df.to_clipboard(index=False, header=False)

        print("URLs have been copied to the clipboard.")

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
