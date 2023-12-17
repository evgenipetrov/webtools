import datetime
import logging
import os

from dateutil.relativedelta import relativedelta

from base_models.base_export_manager import BaseExportManager
from services.google_analytics_service import GoogleAnalytics4Service

logger = logging.getLogger(__name__)

# Export Variables
EXPORT_SUBFOLDER = "googleanalytics4_last_1m_export"
EXPORT_FILENAME = EXPORT_SUBFOLDER + ".csv"
BASE_DATE = datetime.date.today() - relativedelta(days=5)
START_DATE = BASE_DATE - relativedelta(months=1)
END_DATE = BASE_DATE


class GoogleAnalytics4Last1mExport(BaseExportManager):
    def __init__(self, project):
        super().__init__(project, EXPORT_SUBFOLDER)
        self.ga4_auth_domain = None
        self.ga4_property_id = None

    def perform_pre_export_action(self):
        """
        Obtain or confirm authentication details for Google Analytics 4.
        If 'force' is True and auth details are present, skip user input.
        """
        # Check if auth_domain and property_id are already set in the project
        if self.project.ga4_auth_domain and self.project.ga4_property_id:
            if self.force:
                self.ga4_auth_domain = self.project.ga4_auth_domain
                self.ga4_property_id = self.project.ga4_property_id
            else:
                use_existing = input(f"Use existing GA4 settings? (Auth Domain: {self.project.ga4_auth_domain}, Property: {self.project.ga4_property_id}) [Y/n]: ")
                if use_existing.lower() != "n":
                    self.ga4_auth_domain = self.project.ga4_auth_domain
                    self.ga4_property_id = self.project.ga4_property_id
                else:
                    self._gather_user_input()
        else:
            self._gather_user_input()
            # Update project with new values
            self.project.ga4_auth_domain = self.ga4_auth_domain
            self.project.ga4_property_id = self.ga4_property_id
            self.project.save()

    def _gather_user_input(self):
        """
        Gather user input for auth domain and GSC property.
        """
        self.ga4_auth_domain = input("Please provide auth domain for Google Analytics 4: ")
        self.ga4_property_id = input("Please provide the GA4 property ID: ")

    def perform_export(self):
        """
        Implement the actual export logic here
        """
        ga4_service = GoogleAnalytics4Service(self.ga4_auth_domain)

        start_date = START_DATE
        end_date = END_DATE

        # Dimensions & metrics for the export - Adjust as needed
        dimensions = [{"name": "pagePath"}, {"name": "sessionDefaultChannelGrouping"}]
        metrics = [
            {"name": "sessions"},
            {"name": "activeUsers"},
            {"name": "averageSessionDuration"},
            {"name": "bounceRate"},
            {"name": "engagedSessions"},
            {"name": "totalRevenue"},
            {"name": "conversions"},
        ]

        # Fetch and export the data
        df = ga4_service.fetch_data(self.ga4_property_id, start_date, end_date, dimensions, metrics)

        # Save DataFrame to CSV in export folder
        csv_file_path = os.path.join(self.export_path, EXPORT_FILENAME)
        df.to_csv(csv_file_path, index=False)

    def perform_post_export_action(self):
        """
        Any post-export actions, such as logging or confirmation.
        """
