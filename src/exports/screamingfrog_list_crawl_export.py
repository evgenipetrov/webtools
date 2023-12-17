import logging

from base_models.base_export_manager import BaseExportManager

logger = logging.getLogger(__name__)
EXPORT_SUBFOLDER = "screamingfrog_list_crawl_export"


class ScreamingFrogListCrawlExport(BaseExportManager):
    def __init__(self, project, urls=None):
        super().__init__(project, EXPORT_SUBFOLDER)
        self.urls = urls

    def perform_pre_export_action(self):
        """
        Provides instructions for Screaming Frog list crawl export in a step-by-step format.
        """
        # Check if the user wants to proceed
        if not self.force and not self.confirm_export(EXPORT_SUBFOLDER):
            print("Export process aborted.")
            return  # Stop the method if user does not confirm

        # ANSI escape codes for colors
        color_yellow = "\033[93m"
        color_reset = "\033[0m"

        # Copy URLs to clipboard
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
