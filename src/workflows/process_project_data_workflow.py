import logging

from core.models import Project
from data_processors.url_data_processor import UrlDataProcessor

logger = logging.getLogger(__name__)


class ProcessProjectDataWorkflow:
    def __init__(self, project: Project):
        self._project = project

        self.url_data_processor = UrlDataProcessor(project)

    def run(self):
        """
        Main method to run the workflow.
        """
        logger.info(f"Starting data processing for project: {self._project.name}")
        self._process_data()
        logger.info(f"Data processing completed for project: {self._project.name}")

    def _process_data(self):
        """
        Processes the retrieved data.
        """
        # self._process_url_data()
        self.url_data_processor.run()
