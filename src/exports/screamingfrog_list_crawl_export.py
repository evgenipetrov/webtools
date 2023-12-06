import pandas as pd

from core.managers.url_manager import UrlManager
from core.managers.website_manager import WebsiteManager
from exports.base_export_manager import BaseExportManager
import logging

logger = logging.getLogger(__name__)
EXPORT_SUBFOLDER = "screamingfrog_list_crawl_export"


class ScreamingFrogListCrawlExport(BaseExportManager):
    def __init__(self, project, urls):
        super().__init__(project, EXPORT_SUBFOLDER)
        self.urls = urls

    def perform_pre_export_action(self):
        """
        Provides instructions for Screaming Frog list crawl export.
        """
        self.urls.to_clipboard(index=False, header=False)

        print(f"The urls to crawl are copied to clipboard.")
        print(f"Please export the Screaming Frog list crawl data as CSV. Verify for 429 error codes.")
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

