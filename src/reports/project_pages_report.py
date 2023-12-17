from core.models.url import UrlManager
from core.models.website import WebsiteManager
from reports.base_report_manager import BaseReportManager

REPORT_FILENAME = "project_pages_report.csv"
REPORT_COLUMNS = [
    "full_address",
    "relative_address",
    "meta_title",
    "meta_description",
]


class ProjectPagesReport(BaseReportManager):
    def __init__(self, project, report_subfolder="_reports"):
        super().__init__(project, report_subfolder)
        self._report_data = []
        self._report_columns = REPORT_COLUMNS
        self._report_filename = REPORT_FILENAME

    def prepare_report_base(self):
        """
        Prepare the environment before generating the report.
        This could involve initializing or clearing the report data list.
        """
        website = WebsiteManager.get_website_by_project(self._project)
        urls = UrlManager.get_url_by_website(website)
        for url in urls:
            self._report_data.append(
                {
                    "full_address": url.full_address,
                    "relative_address": url.relative_address,
                }
            )

    def generate_report(self):
        """
        The main report generation logic using Django models.
        """
        pass
