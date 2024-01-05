import contextlib
import logging
import os
import sys

from django.conf import settings

from base_models.base_export_manager import BaseExportManager
from services.screamingfrogseospider_service import ScreamingFrogSeoSpiderService

logger = logging.getLogger(__name__)
EXPORT_SUBFOLDER = "screamingfrog_list_crawl_export"
EXPORT_TABS = "Internal:HTML"

CRAWL_LIST_FILENAME = "urls.txt"


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


class ScreamingFrogListCrawlManualExport(BaseExportManager):
    def __init__(self, project, urls=None):
        super().__init__(project, EXPORT_SUBFOLDER)
        self.urls = urls

    def perform_pre_export_action(self):
        """
        Provides instructions for Screaming Frog list crawl export in a step-by-step format.
        """
        # ANSI escape codes for colors
        color_yellow = "\033[93m"
        color_reset = "\033[0m"

        # Copy URLs to clipboard
        # Usage
        with suppress_stdout():
            self.urls.to_clipboard(index=False, header=False)

        instructions = [
            "The URLs to crawl have been copied to the clipboard.",
            "Export the Screaming Frog list crawl data as CSV. Verify for 429 error codes.",
            f"Place the exported file(s) in the following directory: {self.export_path}",
        ]

        print(f"{color_yellow}Please follow these steps for Screaming Frog list crawl export:{color_reset}")
        for i, instruction in enumerate(instructions, start=1):
            print(f"{color_yellow}{i}. {instruction}{color_reset}")

        print(f"\n{color_yellow}After completing these steps, press ENTER to continue.{color_reset}")
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


class ScreamingFrogListCrawlAutomaticExport(BaseExportManager):
    def __init__(self, project, urls=None):
        super().__init__(project, EXPORT_SUBFOLDER)
        self.urls = urls
        self.crawl_list_file_path = os.path.join(settings.TEMP_DIR, CRAWL_LIST_FILENAME)

    def perform_pre_export_action(self):
        """
        Provides instructions for Screaming Frog list crawl export in a step-by-step format.
        """
        # cleanup export folder
        self.empty_export_folder()

        # Create a file with the URLs
        with open(CRAWL_LIST_FILENAME, "w") as file:
            file.write("\n".join(self.urls))

    def perform_export(self):
        """
        Implement the actual export logic here.
        """
        # Export logic or automated steps specific to Screaming Frog
        sf_service = ScreamingFrogSeoSpiderService()
        sf_service.set_crawl_config("/seospiderconfig/listcrawl.seospiderconfig")
        docker_crawl_list_file_path = os.path.join("/export", CRAWL_LIST_FILENAME)
        sf_service.set_crawl_list(docker_crawl_list_file_path)
        sf_service.set_export_tabs(EXPORT_TABS)

        sf_service.run()

    def perform_post_export_action(self):
        """
        Any post-export actions.
        """
        # Delete the crawl list file
        if os.path.exists(self.crawl_list_file_path):
            os.remove(self.crawl_list_file_path)
