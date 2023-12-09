from exports.googlesearchconsole_custom_export import GoogleSearchConsoleCustomExport
from exports.googlesearchconsole_date_page_query_last_16m_export import GoogleSearchConsoleLast16mDatePageQueryExport
from exports.googlesearchconsole_page_last_16m_export import GoogleSearchConsolePageLast16mExport
from exports.googlesearchconsole_last_16m_page_query_export import GoogleSearchConsoleLast16mPageQueryExport
from exports.screamingfrog_custom_export import ScreamingFrogCustomExport
from exports.screamingfrog_list_crawl_export import ScreamingFrogListCrawlExport
from exports.screamingfrog_sitemap_crawl_export import ScreamingFrogSitemapCrawlExport
from exports.screamingfrog_spider_crawl_export import ScreamingFrogSpiderCrawlExport
from exports.semrush_analytics_backlinks_rootdomain_export import SemrushAnalyticsBacklinksRootdomainExport
from exports.semrush_analytics_backlinks_url_export import SemrushAnalyticsBacklinksUrlExport
from exports.semrush_analytics_organic_competitors import SemrushAnalyticsOrganicCompetitorsExport
from exports.semrush_analytics_organic_pages_export import SemrushAnalyticsOrganicPagesExport
from exports.semrush_analytics_organic_positions_rootdomain import SemrushAnalyticsOrganicPositionsRootdomainExport
from exports.sitebulb_spider_crawl_url_internal_export import SitebulbSpiderCrawlUrlInternalExport
from core.models.project import ProjectManager
import logging

from reports.googlesearchconsole_custom_report import GoogleSearchConsoleCustomReport

logger = logging.getLogger(__name__)


class CustomWorkflow:
    def __init__(self, project):
        self.project_id = project.id

    def execute(self):
        project = ProjectManager.get_project_by_id(project_id=self.project_id)

        googlesearchconsole_custom_export = GoogleSearchConsoleCustomExport(project)
        googlesearchconsole_custom_export.run()

        screamingfrog_custom_export = ScreamingFrogCustomExport(project)
        screamingfrog_custom_export.run()

        googlesearchconsole_custom_report = GoogleSearchConsoleCustomReport(project)
        googlesearchconsole_custom_report.run2()
