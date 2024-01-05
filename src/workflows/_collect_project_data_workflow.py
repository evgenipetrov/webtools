import logging

from core.models.project import ProjectManager
from exports.googlesearchconsole_date_page_query_last_16m_export import GoogleSearchConsoleLast16mDatePageQueryExport
from exports.googlesearchconsole_last_16m_page_query_export import GoogleSearchConsoleLast16mPageQueryExport
from exports.googlesearchconsole_page_last_16m_export import GoogleSearchConsolePageLast16mExport
from exports.screamingfrog_list_crawl_export import ScreamingFrogListCrawlExport
from exports.screamingfrog_sitemap_crawl_export import ScreamingFrogSitemapCrawlManualExport
from exports.screamingfrog_spider_crawl_export import ScreamingFrogSpiderCrawlManualExport
from exports.semrush_analytics_backlinks_rootdomain_export import SemrushAnalyticsBacklinksRootdomainExport
from exports.semrush_analytics_backlinks_url_export import SemrushAnalyticsBacklinksUrlExport
from exports.semrush_analytics_organic_competitors import SemrushAnalyticsOrganicCompetitorsExport
from exports.semrush_analytics_organic_pages_export import SemrushAnalyticsOrganicPagesExport
from exports.semrush_analytics_organic_positions_rootdomain import SemrushAnalyticsOrganicPositionsRootdomainExport
from exports.sitebulb_spider_crawl_url_internal_export import SitebulbSpiderCrawlUrlInternalExport

logger = logging.getLogger(__name__)


class CollectProjectDataWorkflow:
    def __init__(self, project):
        self.project_id = project.id

    def execute(self):
        project = ProjectManager.get_project_by_id(project_id=self.project_id)

        screamingfrog_sitemap_crawl_export = ScreamingFrogSitemapCrawlManualExport(project)
        screamingfrog_sitemap_crawl_export.run()

        screamingfrog_spider_crawl_export = ScreamingFrogSpiderCrawlManualExport(project)
        screamingfrog_spider_crawl_export.run()

        sitebulb_url_internal_export = SitebulbSpiderCrawlUrlInternalExport(project)
        sitebulb_url_internal_export.run()

        semrush_analytics_organic_positions_rootdomain_export = SemrushAnalyticsOrganicPositionsRootdomainExport(project)
        semrush_analytics_organic_positions_rootdomain_export.run()

        semrush_analytics_organic_competitors_export = SemrushAnalyticsOrganicCompetitorsExport(project)
        semrush_analytics_organic_competitors_export.run()

        semrush_analytics_organic_pages_export = SemrushAnalyticsOrganicPagesExport(project)
        semrush_analytics_organic_pages_export.run()

        semrush_analytics_backlinks_rootdomain_export = SemrushAnalyticsBacklinksRootdomainExport(project)
        semrush_analytics_backlinks_rootdomain_export.run()

        semrush_analytics_backlinks_url_export = SemrushAnalyticsBacklinksUrlExport(project)
        semrush_analytics_backlinks_url_export.run()

        googlesearchconsole_last_16m_page_export = GoogleSearchConsolePageLast16mExport(project)
        googlesearchconsole_last_16m_page_export.run()

        googlesearchconsole_last_16m_page_query_export = GoogleSearchConsoleLast16mPageQueryExport(project)
        googlesearchconsole_last_16m_page_query_export.run()

        googlesearchconsole_last_16m_date_page_query_export = GoogleSearchConsoleLast16mDatePageQueryExport(project)
        googlesearchconsole_last_16m_date_page_query_export.run()

        screamingfrog_list_crawl_export = ScreamingFrogListCrawlExport(project)
        screamingfrog_list_crawl_export.run()
