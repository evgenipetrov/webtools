from django.core.management.base import BaseCommand
from workflows.collect_project_data_workflow import CollectProjectDataWorkflow
from core.managers.project_manager import ProjectManager
from workflows.update_project_urls import UpdateProjectUrlsWorkflow
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Collects data exports for a specified project by name."

    def add_arguments(self, parser):
        parser.add_argument(
            "--name",
            type=str,
            help="Name of the project for which to collect data exports",
            required=True,
        )

    def handle(self, *args, **options):
        project_name = options["name"]
        project = ProjectManager.get_project_by_name(project_name)

        if not project:
            self.stdout.write(self.style.ERROR(f"Project '{project_name}' not found."))
            return

        workflow1 = CollectProjectDataWorkflow(project)
        workflow1.execute()

        workflow2 = UpdateProjectUrlsWorkflow(project)
        workflow2.execute()

        self.stdout.write(self.style.SUCCESS(f"Data collection for project '{project_name}' completed."))
