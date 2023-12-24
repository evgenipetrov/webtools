import logging

from django.core.management.base import BaseCommand

from core.models.project import ProjectManager
from workflows.run_project_reports_workflow import RunProjectReportsWorkflow

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Runs all reports and stores data in csv files."

    def add_arguments(self, parser):
        parser.add_argument(
            "--project-name",
            type=str,
            help="Name of the project to run reports for",
            required=True,
        )

    def handle(self, *args, **options):
        project_name = options["project_name"]
        project = ProjectManager.get_project_by_name(project_name)

        if not project:
            error_message = f"Project '{project_name}' not found."
            logger.error(error_message)
            raise ValueError(error_message)

        workflow = RunProjectReportsWorkflow(project)
        workflow.run()
