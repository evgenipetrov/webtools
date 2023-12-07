import logging

from django.core.management.base import BaseCommand

from core.models.project import ProjectManager
from reports.website_performance_report import WebsitePerformanceReport

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Runs website performance report."

    def add_arguments(self, parser):
        parser.add_argument(
            "--project-name",
            type=str,
            help="Name of the project to generate report for",
            required=True,
        )

    def handle(self, *args, **options):
        project_name = options["project_name"]
        project = ProjectManager.get_project_by_name(project_name)

        if not project:
            project = ProjectManager.create_project_by_name(project_name)

        report = WebsitePerformanceReport(project)
        report.run()

        logger.info(f"Report run for '{project_name}' completed.")
