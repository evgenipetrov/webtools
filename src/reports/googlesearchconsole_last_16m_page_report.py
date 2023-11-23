import logging
from datetime import datetime, timedelta
from typing import Any

from core.managers.url_manager import UrlManager
from core.managers.website_manager import WebsiteManager
from core.models import Project
from exports.googlesearchconsole_last_16m_page_export import GoogleSearchConsoleLast16mPageExport
from reports.base_report_generator import BaseReportGenerator


class GoogleSearchConsoleLast16mPageReport(BaseReportGenerator):
    """
    Report generator for Google Search Console Last 16 Months Page Report.
    """

    def __init__(self, project: Project) -> None:
        self.project = project

    def run(self) -> None:
        website = WebsiteManager.get_website_by_project(self.project)
        urls = UrlManager.get_urls_by_website(website)
        for url in urls:
            print(url.full_address)
