import logging

from core.models import Project
from data_processors.gscpage_data_processor import GscPageDataProcessor
from data_processors.gscquery_data_processor import GscQueryDataProcessor
from data_processors.keywordranking_data_processor import KeywordRankingDataProcessor
from data_processors.url_data_processor import UrlDataProcessor

logger = logging.getLogger(__name__)


class ProcessProjectDataWorkflow:
    def __init__(self, project: Project):
        self._project = project

        self.url_data_processor = UrlDataProcessor(project)
        self.gscpage_data_processor = GscPageDataProcessor(project)
        self.gscquery_data_processor = GscQueryDataProcessor(project)
        self.keywordranking_data_processor = KeywordRankingDataProcessor(project)

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
        # todo activate processors after debug sessions
        self.url_data_processor.run()
        self.gscpage_data_processor.run()
        self.gscquery_data_processor.run()
        self.keywordranking_data_processor.run()
