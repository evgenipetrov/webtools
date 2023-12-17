import logging

from base_models.base_report_manager import BaseReportManager
from core.models.url import UrlManager
from core.models.website import WebsiteManager

REPORT_FILENAME = "project_pages_report.csv"


logger = logging.getLogger(__name__)


class ReportColumns:
    FULL_ADDRESS = "full_address"
    RELATIVE_ADDRESS = "relative_address"
    STATUS_CODE = "status_code"
    INDEXABILITY = "indexability"
    INDEXABILITY_STATUS = "indexability_status"
    REDIRECT_TO = "redirect_to"
    META_TITLE = "meta_title"
    META_DESCRIPTION = "meta_description"
    H1 = "h1"
    CANONICAL_LINK = "canonical_link"
    PAGE_TEMPLATE = "page_template"
    IN_SITEMAP = "in_sitemap"

    @classmethod
    def get_columns(cls):
        return [
            cls.FULL_ADDRESS,
            cls.RELATIVE_ADDRESS,
            cls.STATUS_CODE,
            cls.INDEXABILITY,
            cls.INDEXABILITY_STATUS,
            cls.REDIRECT_TO,
            cls.META_TITLE,
            cls.META_DESCRIPTION,
            cls.H1,
            cls.CANONICAL_LINK,
            cls.PAGE_TEMPLATE,
            cls.IN_SITEMAP,
        ]


class ProjectPagesReport(BaseReportManager):
    def __init__(self, project, report_subfolder="_reports"):
        super().__init__(project, report_subfolder)
        self._report_data = []
        self._report_columns = ReportColumns.get_columns()
        self._report_filename = REPORT_FILENAME

    def prepare_report_base(self):
        """
        Prepare the key column of the report.
        """
        logger.info("Preparing report base.")
        try:
            website = WebsiteManager.get_website_by_project(self._project)
            urls = UrlManager.get_urls_by_website(website)
            for url in urls:
                self._report_data.append(
                    {
                        ReportColumns.FULL_ADDRESS: url.full_address,
                    }
                )
            logger.info("Report base prepared successfully.")
        except Exception as e:
            logger.error(f"Error in preparing report base: {e}")
            raise

    def generate_report(self):
        """
        Augments each row with additional data, one column at a time.
        """
        logger.info("Generating report data.")
        try:
            for row in self._report_data:
                logger.info(f"Generating report data for row with {ReportColumns.FULL_ADDRESS}: {row[ReportColumns.FULL_ADDRESS]}")
                row[ReportColumns.RELATIVE_ADDRESS] = UrlManager.get_relative_address(row[ReportColumns.FULL_ADDRESS])
                row[ReportColumns.STATUS_CODE] = UrlManager.get_status_code(row[ReportColumns.FULL_ADDRESS])
                row[ReportColumns.INDEXABILITY] = UrlManager.get_indexability(row[ReportColumns.FULL_ADDRESS])
                row[ReportColumns.INDEXABILITY_STATUS] = UrlManager.get_indexability_status(row[ReportColumns.FULL_ADDRESS])
                row[ReportColumns.REDIRECT_TO] = UrlManager.get_redirect_to(row[ReportColumns.FULL_ADDRESS])
                row[ReportColumns.META_TITLE] = UrlManager.get_meta_title(row[ReportColumns.FULL_ADDRESS])
                row[ReportColumns.META_DESCRIPTION] = UrlManager.get_meta_description(row[ReportColumns.FULL_ADDRESS])
                row[ReportColumns.H1] = UrlManager.get_h1(row[ReportColumns.FULL_ADDRESS])
                row[ReportColumns.CANONICAL_LINK] = UrlManager.get_canonical_link(row[ReportColumns.FULL_ADDRESS])
                row[ReportColumns.PAGE_TEMPLATE] = UrlManager.get_page_template(row[ReportColumns.FULL_ADDRESS])
                row[ReportColumns.IN_SITEMAP] = UrlManager.get_in_sitemap(row[ReportColumns.FULL_ADDRESS])
            logger.info("Report data generated successfully.")
        except Exception as e:
            logger.error(f"Error in report data generation: {e}")
            raise
