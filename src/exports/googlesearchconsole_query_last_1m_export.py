import datetime
import logging
import os

from dateutil.relativedelta import relativedelta  # Provides more accurate date manipulation

from base_models.base_export_manager import BaseExportManager
from services.google_search_console_service import GoogleSearchConsoleService

logger = logging.getLogger(__name__)
# Export Variables
EXPORT_SUBFOLDER = "googlesearchconsole_query_last_1m_export"
EXPORT_FILENAME = EXPORT_SUBFOLDER + ".csv"
BASE_DATE = datetime.date.today() - relativedelta(days=5)
START_DATE = BASE_DATE - relativedelta(months=1)
END_DATE = BASE_DATE


class GoogleSearchConsoleQueryLast1mExport(BaseExportManager):
    def __init__(self, project):
        super().__init__(project, EXPORT_SUBFOLDER)
        self.gsc_auth_email = None
        self.gsc_property_name = None

    def perform_pre_export_action(self):
        """
        Obtain or confirm authentication details for Google Search Console.
        If 'self.force' is True and auth details are present, skip user input.
        """
        # Check if auth_email and gsc_property are already set in the project
        if self.project.gsc_auth_email and self.project.gsc_property_name:
            if self.force:
                self.gsc_auth_email = self.project.gsc_auth_email
                self.gsc_property_name = self.project.gsc_property_name
            else:
                use_existing = input(f"Use existing GSC settings? (Auth Email: {self.project.gsc_auth_email}, Property: {self.project.gsc_property_name}) [Y/n]: ")
                if use_existing.lower() != "n":
                    self.gsc_auth_email = self.project.gsc_auth_email
                    self.gsc_property_name = self.project.gsc_property_name
                else:
                    self._gather_user_input()
        else:
            self._gather_user_input()
            # Update project with new values
            self.project.gsc_auth_email = self.gsc_auth_email
            self.project.gsc_property_name = self.gsc_property_name
            self.project.save()

    def _gather_user_input(self):
        """
        Gather user input for auth email and GSC property.
        """
        self.gsc_auth_email = input("Please provide auth email for Google Search Console: ")
        self.gsc_property_name = input("Please provide the GSC property URL: ")

    def perform_export(self):
        """
        Implement the actual export logic here, utilizing GoogleSearchConsoleService.
        """
        gsc_service = GoogleSearchConsoleService(self.gsc_auth_email)

        start_date = START_DATE
        end_date = END_DATE

        # Dimensions for the export - Adjust as needed
        dimensions = ["query"]

        # Fetch and export the data
        df = gsc_service.fetch_data(self.gsc_property_name, start_date, end_date, dimensions)

        # Save DataFrame to CSV in export folder
        csv_file_path = os.path.join(self.export_path, EXPORT_FILENAME)
        df.to_csv(csv_file_path, index=False)

    def perform_post_export_action(self):
        """
        Any post-export actions, such as logging or confirmation.
        """
