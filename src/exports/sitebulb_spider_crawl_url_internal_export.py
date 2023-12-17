import logging

from base_models.base_export_manager import BaseExportManager

logger = logging.getLogger(__name__)
EXPORT_SUBFOLDER = "sitebulb_spider_crawl_url_internal_export"


class SitebulbSpiderCrawlUrlInternalExport(BaseExportManager):
    def __init__(self, project):
        super().__init__(project, EXPORT_SUBFOLDER)

    def perform_pre_export_action(self):
        """
        Provide instructions for Sitebulb URL internal crawl export in a step-by-step format.
        """
        # Check if the user wants to proceed
        if not self.force and not self.confirm_export(EXPORT_SUBFOLDER):
            print("Export process aborted.")
            return  # Stop the method if user does not confirm

        # ANSI escape codes for colors
        color_yellow = "\033[93m"
        color_reset = "\033[0m"

        instructions = [
            "Export the Sitebulb spider data. Export Url -> Internal -> All as CSV using default settings.",
            "Make sure the 'Redirect URL' column is added to the report.",
            "Verify for 429 error codes in the export.",
            f"Place the exported file(s) in the following directory: {self.export_path}",
        ]

        print(f"{color_yellow}Please follow these steps for Sitebulb URL internal crawl export:{color_reset}")
        for i, instruction in enumerate(instructions, start=1):
            print(f"{color_yellow}{i}. {instruction}{color_reset}")

        print("\nAfter completing these steps:")
        input("Press ENTER to continue after placing the exported files.")

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
