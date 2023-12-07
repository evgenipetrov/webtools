import datetime
import os

from dateutil.relativedelta import relativedelta  # Provides more accurate date manipulation

from core.managers.url_manager import UrlManager
from core.managers.website_manager import WebsiteManager
from exports.base_export_manager import BaseExportManager
from services.google_search_console_service import GoogleSearchConsoleService
import logging

logger = logging.getLogger(__name__)
EXPORT_SUBFOLDER = "googlesearchconsole_page_query_previous_1m_export"


class GoogleSearchConsolePageQueryExportPrevious1m(BaseExportManager):
    def __init__(self, project):
        super().__init__(project, EXPORT_SUBFOLDER)
        self.gsc_auth_domain = None
        self.gsc_property_name = None

    def perform_pre_export_action(self):
        """
        Obtain or confirm authentication details for Google Search Console.
        """
        # Check if auth_domain and gsc_property are already set in the project
        if self.project.gsc_auth_domain and self.project.gsc_property_name:
            use_existing = input(f"Use existing GSC settings? (Auth Domain: {self.project.gsc_auth_domain}, Property: {self.project.gsc_property_name}) [Y/n]: ")
            if use_existing.lower() != "n":
                self.gsc_auth_domain = self.project.gsc_auth_domain
                self.gsc_property_name = self.project.gsc_property_name
            else:
                self._gather_user_input()
        else:
            self._gather_user_input()
            # Update project with new values
            self.project.gsc_auth_domain = self.gsc_auth_domain
            self.project.gsc_property_name = self.gsc_property_name
            self.project.save()

    def _gather_user_input(self):
        """
        Gather user input for auth domain and GSC property.
        """
        self.gsc_auth_domain = input("Please provide auth domain for Google Search Console: ")
        self.gsc_property_name = input("Please provide the GSC property URL: ")

    def perform_export(self):
        """
        Implement the actual export logic here, utilizing GoogleSearchConsoleService.
        """
        gsc_service = GoogleSearchConsoleService(self.gsc_auth_domain)
        end_date = datetime.date.today() - relativedelta(months=1)
        start_date = datetime.date.today() - relativedelta(months=2)

        # Dimensions for the export - Adjust as needed
        dimensions = ["page", "query"]

        # Fetch and export the data
        df = gsc_service.fetch_data(self.gsc_property_name, start_date, end_date, dimensions)

        # Save DataFrame to CSV in export folder
        csv_file_path = os.path.join(self.export_path, "gsc_previous_1m_page_query_export.csv")
        df.to_csv(csv_file_path, index=False)

        print(f"Exported GSC data to {csv_file_path}")

    def perform_post_export_action(self):
        """
        Any post-export actions, such as logging or confirmation.
        """
        print("Export from Google Search Console completed.")

