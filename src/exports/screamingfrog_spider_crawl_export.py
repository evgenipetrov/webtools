from exports.base_export_manager import BaseExportManager
import logging

logger = logging.getLogger(__name__)
EXPORT_SUBFOLDER = "screamingfrog_spider_crawl_export"


class ScreamingFrogSpiderCrawlExport(BaseExportManager):
    def __init__(self, project):
        super().__init__(project, EXPORT_SUBFOLDER)

    def perform_pre_export_action(self):
        """
        Provide instructions for Screaming Frog website crawl export.
        """
        print(f"Please export the Screaming Frog spider crawl data as CSV using default settings. Verify for 429 error codes.")
        print(f"Place the exported file(s) in the following directory: {self.export_folder}")

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
        # Example: Confirmation or cleanup steps
        input("Press ENTER to continue after placing the exported files.")
