from core.managers.url_manager import UrlManager
from core.managers.website_manager import WebsiteManager
from exports.base_export_manager import BaseExportManager
import logging

logger = logging.getLogger(__name__)
EXPORT_SUBFOLDER = "semrush_analytics_organic_positions_rootdomain"


class SemrushAnalyticsOrganicPositionsRootdomainExport(BaseExportManager):
    def __init__(self, project):
        super().__init__(project, EXPORT_SUBFOLDER)

    def perform_pre_export_action(self):
        """
        Provides instructions for Semrush organic positions export in a step-by-step format.
        """
        # Check if the user wants to proceed
        if not self.force and not self.confirm_export(EXPORT_SUBFOLDER):
            print("Export process aborted.")
            return  # Stop the method if user does not confirm

        # ANSI escape codes for colors
        color_yellow = "\033[93m"
        color_blue = "\033[94m"
        color_reset = "\033[0m"

        domain = UrlManager.get_root_url(self.project.base_url)
        url = f"https://www.semrush.com/analytics/organic/positions/?sortField=&sortDirection=desc&db=us&q={domain}&searchType=domain"

        instructions = [
            f"Open the following URL and perform export: {color_blue}{url}{color_reset}",
            f"Place the exported file(s) in the following directory: {self.export_path}",
        ]

        print(f"{color_yellow}Please follow these steps for Semrush organic positions export:{color_reset}")
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
