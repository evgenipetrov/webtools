import logging

from base_models.base_export_manager import BaseExportManager
from core.models.url import UrlManager

logger = logging.getLogger(__name__)
EXPORT_SUBFOLDER = "semrush_analytics_backlinks_rootdomain"


class SemrushAnalyticsBacklinksRootdomainExport(BaseExportManager):
    def __init__(self, project):
        super().__init__(project, EXPORT_SUBFOLDER)

    def perform_pre_export_action(self):
        """
        Provides instructions for Semrush backlinks export for root domain in a step-by-step format.
        """
        # ANSI escape codes for colors
        color_yellow = "\033[93m"
        color_blue = "\033[94m"
        color_reset = "\033[0m"

        domain = UrlManager.get_root_url(self.project.base_url)
        url = f"https://www.semrush.com/analytics/backlinks/backlinks/?q={domain}&searchType=domain"

        instructions = [
            f"Open the following URL and perform export: {color_blue}{url}{color_reset}",
            f"Place the exported file(s) in the following directory: {self.export_path}",
        ]

        print(f"{color_yellow}Please follow these steps for Semrush backlinks export:{color_reset}")
        for i, instruction in enumerate(instructions, start=1):
            print(f"{color_yellow}{i}. {instruction}{color_reset}")

        print(f"\n{color_yellow}After completing these steps, press ENTER to continue.{color_reset}")
        input("Press ENTER to continue after placing the exported files.")

    def perform_export(self):
        """
        Implement the actual export logic here.
        """
        pass

    def perform_post_export_action(self):
        """
        Any post-export actions.
        """
