import logging

from core.models import Project
from reports.project_pages_report import ProjectPagesReport

logger = logging.getLogger(__name__)


class RunProjectReportsWorkflow:
    project_pages_report: ProjectPagesReport

    def __init__(self, project: Project):
        self._project = project

    def run(self):
        """
        Main method to run the workflow.
        """
        logger.info(f"Starting report run for project: {self._project.name}")
        self._run_reports()
        logger.info(f"Report run completed for project: {self._project.name}")

    def _run_reports(self):
        """
        Processes the retrieved data.
        """
        self._project_pages_report = ProjectPagesReport(self._project)
        self._project_pages_report.run()
