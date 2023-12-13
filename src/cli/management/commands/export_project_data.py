import logging

from django.core.management.base import BaseCommand

from core.models.project import ProjectManager
from workflows.export_project_data_workflow import ExportProjectDataWorkflow

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Runs all exports."

    def add_arguments(self, parser):
        parser.add_argument(
            "--project-name",
            type=str,
            help="Name of the project to run exports for",
            required=True,
        )

    def handle(self, *args, **options):
        project_name = options["project_name"]
        project = ProjectManager.get_project_by_name(project_name)

        if not project:
            project = ProjectManager.create_project_by_name_dialog(project_name)
        else:
            ProjectManager.update_project_details_dialog(project)

        workflow = ExportProjectDataWorkflow(project)
        workflow.run()
