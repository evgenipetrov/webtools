from django.core.management.base import BaseCommand

from workflows.custom_workflow import CustomWorkflow
from workflows.update_project_urls import UpdateProjectUrlsWorkflow
from core.models.project import ProjectManager
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Runs special report."

    def add_arguments(self, parser):
        parser.add_argument(
            "--project-name",
            type=str,
            help="Name of the project for which to collect data exports",
            required=True,
        )

    def handle(self, *args, **options):
        project_name = options["project_name"]
        project = ProjectManager.get_project_by_name(project_name)

        if not project:
            self.stdout.write(self.style.ERROR(f"Project '{project_name}' not found."))
            return

        custom_workflow = CustomWorkflow(project)
        custom_workflow.execute()

        self.stdout.write(self.style.SUCCESS(f"Url update for project '{project_name}' completed."))
