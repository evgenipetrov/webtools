from core.managers.url_manager import UrlManager
from exports.base_export_manager import BaseExportManager
import logging

logger = logging.getLogger(__name__)
EXPORT_SUBFOLDER = "semrush_analytics_organic_positions_rootdomain"


class SemrushAnalyticsOrganicPositionsRootdomainExport(BaseExportManager):
    def __init__(self, project):
        super().__init__(project, EXPORT_SUBFOLDER)

    def perform_pre_export_action(self):
        """
        Provides instructions for Semrush organic positions export.
        """
        domain = UrlManager.get_root_url(self.project.base_url)
        url = f"https://www.semrush.com/analytics/organic/positions/?sortField=&sortDirection=desc&db=us&q={domain}&searchType=domain"
        print(f"Open the following URL and perform export.")
        print(url)
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
        input("Press ENTER to continue after placing the exported files.")
        pass
