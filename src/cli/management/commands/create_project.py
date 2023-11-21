from django.core.management.base import BaseCommand, CommandError

from workflows.create_project_workflow import CreateProjectWorkflow
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Creates a new project"

    def add_arguments(self, parser):
        # Define the arguments that this command will take
        parser.add_argument("--name", type=str, help="Name of the project", required=True)
        parser.add_argument("--url", type=str, help="Base URL of the project", required=True)
        parser.add_argument("--project_data", type=str, help="Path to the data folder", required=True)

    def handle(self, *args, **options):
        name = options["name"]
        url = options["url"]
        data_folder = options["project_data"]

        workflow = CreateProjectWorkflow(
            project_name=name,
            project_url=url,
            project_data_folder=data_folder,
        )

        workflow.execute()
