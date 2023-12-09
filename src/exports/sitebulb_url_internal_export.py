from core.managers.url_manager import UrlManager
from core.managers.website_manager import WebsiteManager
from exports.base_export_manager import BaseExportManager
import logging

logger = logging.getLogger(__name__)
EXPORT_SUBFOLDER = "sitebulb_url_internal_export"


class SitebulbUrlInternalExport(BaseExportManager):
    def __init__(self, project):
        super().__init__(project, EXPORT_SUBFOLDER)

    def perform_pre_export_action(self):
        """
        Provide instructions for Screaming Frog website crawl export.
        """
        print(f"Please export the Sitebulb crawl data. Export Url -> Internal -> All as CSV using default settings.")
        print(f"Make sure Redirect URL column is added to the report. Verify for 429 error codes.")
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
