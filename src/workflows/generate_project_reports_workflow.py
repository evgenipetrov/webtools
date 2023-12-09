from core.models import Project
from exports.googlesearchconsole_page_last_16m_export import GoogleSearchConsolePageLast16mExport
from exports.googlesearchconsole_last_16m_page_query_export import GoogleSearchConsoleLast16mPageQueryExport
from exports.screamingfrog_list_crawl_export import ScreamingFrogListCrawlExport
from exports.screamingfrog_sitemap_crawl_export import ScreamingFrogSitemapCrawlExport
from exports.screamingfrog_spider_crawl_export import ScreamingFrogSpiderCrawlExport
from exports.semrush_analytics_backlinks_rootdomain_export import SemrushAnalyticsBacklinksRootdomainExport
from exports.semrush_analytics_backlinks_url_export import SemrushAnalyticsBacklinksUrlExport
from exports.semrush_analytics_organic_competitors import SemrushAnalyticsOrganicCompetitorsExport
from exports.semrush_analytics_organic_pages_export import SemrushAnalyticsOrganicPagesExport
from exports.semrush_analytics_organic_positions_rootdomain import SemrushAnalyticsOrganicPositionsRootdomainExport
from exports.sitebulb_url_internal_export import SitebulbUrlInternalExport
from core.models.project import ProjectManager
import logging

from reports.googlesearchconsole_last_16m_lead_query_report import GoogleSearchConsoleLast16mLeadQueryReport
from reports.googlesearchconsole_last_16m_page_query_report import GoogleSearchConsoleLast16mPageQueryReport
from reports.googlesearchconsole_last_16m_page_report import GoogleSearchConsoleLast16mPageReport

logger = logging.getLogger(__name__)


class GenerateProjectReportsWorkflow:
    def __init__(self, project: Project):
        self.project = project

    def execute(self):
        project = ProjectManager.get_project_by_id(project_id=self.project.id)

        googlesearchconsole_last_16m_page_report = GoogleSearchConsoleLast16mPageReport(project)
        googlesearchconsole_last_16m_page_report.run2()

        googlesearchconsole_last_16m_page_query_report = GoogleSearchConsoleLast16mPageQueryReport(project)
        googlesearchconsole_last_16m_page_query_report.run2()

        googlesearchconsole_last_16m_lead_query_report = GoogleSearchConsoleLast16mLeadQueryReport(project)
        googlesearchconsole_last_16m_lead_query_report.run2()
