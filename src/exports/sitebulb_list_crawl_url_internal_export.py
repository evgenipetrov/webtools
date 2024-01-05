import contextlib
import logging
import os
import sys

from base_models.base_export_manager import BaseExportManager

logger = logging.getLogger(__name__)
EXPORT_SUBFOLDER = "sitebulb_list_crawl_url_internal_export"


@contextlib.contextmanager
def suppress_stdout():
    """Context manager to suppress standard output."""
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout


class SitebulbListCrawlUrlInternalExport(BaseExportManager):
    def __init__(self, project, urls=None):
        super().__init__(project, EXPORT_SUBFOLDER)
        self.urls = urls

    def perform_pre_export_action(self):
        """
        Provide instructions for Sitebulb URL internal crawl export in a step-by-step format.
        """
        # cleanup export folder
        self.empty_export_folder()

        # ANSI escape codes for colors
        color_yellow = "\033[93m"
        color_reset = "\033[0m"

        # Usage
        with suppress_stdout():
            self.urls.to_clipboard(index=False, header=False)

        instructions = [
            "The urls to crawl are copied to clipboard.",
            "Export the Sitebulb list crawl data. Export Url -> Internal -> All as CSV using default settings.",
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
