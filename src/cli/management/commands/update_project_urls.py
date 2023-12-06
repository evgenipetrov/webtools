from django.core.management.base import BaseCommand
from workflows.update_project_urls import UpdateProjectUrlsWorkflow
from core.models.project import ProjectManager
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Updates project urls."

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

        workflow = UpdateProjectUrlsWorkflow(project)
        workflow.execute()

        self.stdout.write(self.style.SUCCESS(f"Url update for project '{project_name}' completed."))
