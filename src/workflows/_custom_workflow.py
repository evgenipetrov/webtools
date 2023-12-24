import logging

from core.models.project import ProjectManager
from exports.googlesearchconsole_custom_export import GoogleSearchConsoleCustomExport
from exports.screamingfrog_custom_export import ScreamingFrogCustomExport
from reports.googlesearchconsole_custom_report import GoogleSearchConsoleCustomReport

logger = logging.getLogger(__name__)


class CustomWorkflow:
    def __init__(self, project):
        self.project_id = project.id

    def execute(self):
        project = ProjectManager.get_project_by_id(project_id=self.project_id)

        googlesearchconsole_custom_export = GoogleSearchConsoleCustomExport(project)
        googlesearchconsole_custom_export.run()

        screamingfrog_custom_export = ScreamingFrogCustomExport(project)
        screamingfrog_custom_export.run()

        googlesearchconsole_custom_report = GoogleSearchConsoleCustomReport(project)
        googlesearchconsole_custom_report.run2()
