import logging

from core.models import Project
from data_processors.url_data_processor import UrlDataProcessor

logger = logging.getLogger(__name__)


class ProcessProjectDataWorkflow:
    def __init__(self, project: Project):
        self._project = project

        self.url_data_processor = UrlDataProcessor(project)

        # googleanalytics4
        self.googleanalytics4_last_14m_data = None
        self.googleanalytics4_last_1m_data = None
        self.googleanalytics4_last_1m_previous_year_data = None
        self.googleanalytics4_previous_1m_data = None
        # googlesearchconsole
        self.googlesearchconsole_date_page_query_last_16m_data = None
        self.googlesearchconsole_page_last_16m_data = None
        self.googlesearchconsole_page_last_1m_data = None
        self.googlesearchconsole_page_last_1m_previous_year_data = None
        self.googlesearchconsole_page_previous_1m_data = None
        self.googlesearchconsole_page_query_last_16m_data = None
        self.googlesearchconsole_page_query_last_1m_data = None
        self.googlesearchconsole_page_query_last_1m_previous_year_data = None
        self.googlesearchconsole_page_query_previous_1m_data = None
        self.googlesearchconsole_query_last_16m_data = None
        self.googlesearchconsole_query_last_1m_data = None
        self.googlesearchconsole_query_last_1m_previous_year_data = None
        self.googlesearchconsole_query_previous_15m_data = None
        self.googlesearchconsole_query_previous_1m_data = None
        # screamingfrog
        self.screamingfrog_list_crawl_data = None
        self.screamingfrog_sitemap_crawl_data = None
        self.screamingfrog_spider_crawl_data = None
        # semrush
        self.semrush_analytics_backlinks_rootdomain_data = None
        self.semrush_analytics_organic_competitors_data = None
        self.semrush_analytics_organic_pages_data = None
        self.semrush_analytics_organic_positions_rootdomain_data = None
        # sitebulb
        self.sitebulb_list_crawl_url_internal_data = None
        self.sitebulb_spider_crawl_url_internal_data = None

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
