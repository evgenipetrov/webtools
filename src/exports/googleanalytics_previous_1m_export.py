import datetime
import os

from dateutil.relativedelta import relativedelta  # Provides more accurate date manipulation

from core.managers.url_manager import UrlManager
from core.managers.website_manager import WebsiteManager
from exports.base_export_manager import BaseExportManager
from services.google_analytics_service import GoogleAnalytics4Service
from services.google_search_console_service import GoogleSearchConsoleService
import logging

logger = logging.getLogger(__name__)
EXPORT_SUBFOLDER = "googleanalytics4_previous_1m_export"


class GoogleAnalytics4ExportPrevious1m(BaseExportManager):
    def __init__(self, project):
        super().__init__(project, EXPORT_SUBFOLDER)
        self.ga4_auth_domain = None
        self.ga4_property_id = None

    def perform_pre_export_action(self):
        """
        Obtain or confirm authentication details for Google Search Console.
        """
        # Check if auth_domain and gsc_property are already set in the project
        if self.project.ga4_auth_domain and self.project.ga4_property_id:
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
        Implement the actual export logic here, utilizing GoogleSearchConsoleService.
        """
        ga4_service = GoogleAnalytics4Service(self.ga4_auth_domain)
        end_date = datetime.date.today()
        start_date = end_date - relativedelta(months=1)

        # Dimensions & metrics for the export - Adjust as needed
        dimensions = [
            {"name": "pagePath"},
            {"name": "sessionDefaultChannelGrouping"}
        ]
        metrics = [
            {"name": "sessions"},
            {"name": "activeUsers"},
            {"name": "engagedSessions"},
            {"name": "totalRevenue"},  # Optional based on your tracking setup
            {"name": "conversions"},  # Ensure you have conversions set up in GA4
        ]

        # Fetch and export the data
        df = ga4_service.fetch_data(self.ga4_property_id, start_date, end_date, dimensions, metrics)

        # Save DataFrame to CSV in export folder
        csv_file_path = os.path.join(self.export_path, "googleanalytics4_previous_1m_export.csv")
        df.to_csv(csv_file_path, index=False)

        print(f"Exported GA4 data to {csv_file_path}")

    def perform_post_export_action(self):
        """
        Any post-export actions, such as logging or confirmation.
        """
        print("Export from Google Analytics 4 completed.")

