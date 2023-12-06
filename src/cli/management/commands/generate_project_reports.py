from django.core.management.base import BaseCommand
import logging

from core.models.project import ProjectManager

from workflows.generate_project_reports_workflow import GenerateProjectReportsWorkflow

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Generates the _reports."

    def add_arguments(self, parser):
        parser.add_argument(
            "--project-name",
            type=str,
            help="Name of the project for which to generate the _reports",
            required=True,
        )

    def handle(self, *args, **options):
        project_name = options["project_name"]
        project = ProjectManager.get_project_by_name(project_name)

        workflow = GenerateProjectReportsWorkflow(project)
        workflow.execute()

        self.stdout.write(self.style.SUCCESS(f"Google Search Console Page Report for project '{project_name}' generated successfully."))
