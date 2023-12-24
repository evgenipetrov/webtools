import logging

from core.models import Project
from core.models.project import ProjectManager
from reports.googlesearchconsole_last_16m_lead_query_report import GoogleSearchConsoleLast16mLeadQueryReport
from reports.googlesearchconsole_last_16m_page_query_report import GoogleSearchConsoleLast16mPageQueryReport
from reports.googlesearchconsole_last_16m_page_report import GoogleSearchConsoleLast16mPageReport

logger = logging.getLogger(__name__)


class GenerateProjectReportsWorkflow:
    def __init__(self, project: Project):
        self.project = project

    def execute(self):
        project = ProjectManager.get_project_by_id(project_id=self.project.id)

        googlesearchconsole_last_16m_page_report = GoogleSearchConsoleLast16mPageReport(project)
        googlesearchconsole_last_16m_page_report.run2()

        googlesearchconsole_last_16m_page_query_report = GoogleSearchConsoleLast16mPageQueryReport(project)
        googlesearchconsole_last_16m_page_query_report.run2()

        googlesearchconsole_last_16m_lead_query_report = GoogleSearchConsoleLast16mLeadQueryReport(project)
        googlesearchconsole_last_16m_lead_query_report.run2()
