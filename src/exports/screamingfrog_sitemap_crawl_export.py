import logging
import os
import shutil

from django.conf import settings

from base_models.base_export_manager import BaseExportManager
from services.screamingfrogseospider_service import ScreamingFrogSeoSpiderService

logger = logging.getLogger(__name__)
EXPORT_SUBFOLDER = "screamingfrog_sitemap_crawl_export"
EXPORT_TABS = "Internal:HTML"


class ScreamingFrogSitemapCrawlManualExport(BaseExportManager):
    def __init__(self, project):
        super().__init__(project, EXPORT_SUBFOLDER)

    def perform_pre_export_action(self):
        """
        Provides instructions for Screaming Frog sitemap crawl export in a step-by-step format.
        """
        # ANSI escape codes for colors
        color_yellow = "\033[93m"
        color_reset = "\033[0m"

        instructions = [
            "Export the Screaming Frog sitemap crawl data as CSV.",
            "Verify for 429 error codes in the export.",
            f"Place the exported file(s) in the following directory: {self.export_path}",
        ]

        print(f"{color_yellow}Please follow these steps for Screaming Frog sitemap crawl export:{color_reset}")
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


class ScreamingFrogSitemapCrawlAutomaticExport(BaseExportManager):
    def __init__(self, project):
        super().__init__(project, EXPORT_SUBFOLDER)

    def perform_pre_export_action(self):
        """
        Provides instructions for Screaming Frog sitemap crawl export in a step-by-step format.
        """
        # cleanup export folder
        self.empty_export_folder()

    def perform_export(self):
        """
        Implement the actual export logic here.
        """
        # Export logic or automated steps specific to Screaming Frog
        sf_service = ScreamingFrogSeoSpiderService()
        sf_service.set_crawl_config("/seospiderconfig/listcrawl.seospiderconfig")
        sf_service.set_sitemap_url(self.project.sitemap_url)
        sf_service.set_export_tabs(EXPORT_TABS)

        sf_service.run()

    def perform_post_export_action(self):
        """
        Any post-export actions.
        """
        # Move files from TEMP_DIR to export_path
        for filename in os.listdir(settings.TEMP_DIR):
            shutil.move(os.path.join(settings.TEMP_DIR, filename), self.export_path)
