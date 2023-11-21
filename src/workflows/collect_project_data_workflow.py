from exports.googlesearchconsole_last_16m_page_export import GoogleSearchConsoleLast16mPageExport
from exports.googlesearchconsole_last_16m_page_query_export import GoogleSearchConsoleLast16mPageQueryExport
from exports.screamingfrog_list_crawl_export import ScreamingFrogListCrawlExport
from exports.screamingfrog_sitemap_crawl_export import ScreamingFrogSitemapCrawlExport
from exports.screamingfrog_spider_crawl_export import ScreamingFrogSpiderCrawlExport
from exports.semrush_analytics_organic_positions_rootdomain import SemrushAnalyticsOrganicPositionsRootdomainExport
from exports.sitebulb_url_internal_export import SitebulbUrlInternalExport
from core.managers.project_manager import ProjectManager
import logging

logger = logging.getLogger(__name__)


class CollectProjectDataWorkflow:
    def __init__(self, project):
        self.project_id = project.id

    def execute(self):
        project = ProjectManager.get_project(project_id=self.project_id)
        screamingfrog_sitemap_crawl_export = ScreamingFrogSitemapCrawlExport(project)
        screamingfrog_sitemap_crawl_export.collect()

        screamingfrog_spider_crawl_export = ScreamingFrogSpiderCrawlExport(project)
        screamingfrog_spider_crawl_export.collect()

        sitebulb_url_internal_export = SitebulbUrlInternalExport(project)
        sitebulb_url_internal_export.collect()

        semrush_analytics_organic_positions_rootdomain_export = SemrushAnalyticsOrganicPositionsRootdomainExport(project)
        semrush_analytics_organic_positions_rootdomain_export.collect()

        googlesearchconsole_last_16m_page_export = GoogleSearchConsoleLast16mPageExport(project)
        googlesearchconsole_last_16m_page_export.collect()

        googlesearchconsole_last_16m_page_query_export = GoogleSearchConsoleLast16mPageQueryExport(project)
        googlesearchconsole_last_16m_page_query_export.collect()
