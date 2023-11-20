# Assuming this is in cli/management/commands/create_project.py
from django.core.management.base import BaseCommand, CommandError
from core.managers.project_manager import ProjectManager


class Command(BaseCommand):
    help = "Creates a new project"

    def add_arguments(self, parser):
        # Define the arguments that this command will take
        parser.add_argument("name", type=str, help="Name of the project")
        parser.add_argument("base_url", type=str, help="Base URL of the project")
        parser.add_argument("data_folder", type=str, help="Path to the data folder")

    def handle(self, *args, **options):
        name = options["name"]
        base_url = options["base_url"]
        data_folder = options["data_folder"]

        project = ProjectManager.create_project(name, base_url, data_folder)
