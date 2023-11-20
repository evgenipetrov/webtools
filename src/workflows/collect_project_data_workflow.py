from exports.googlesearchconsole_last_16m_page_export import GoogleSearchConsoleLast16mPageExport
from exports.screamingfrog_list_crawl_export import ScreamingFrogListCrawlExport
from exports.screamingfrog_sitemap_crawl_export import ScreamingFrogSitemapCrawlExport
from exports.screamingfrog_spider_crawl_export import ScreamingFrogSpiderCrawlExport
from exports.sitebulb_url_internal_export import SitebulbUrlInternalExport
from core.managers.project_manager import ProjectManager


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

        googlesearchconsole_last_16m_page_export = GoogleSearchConsoleLast16mPageExport(project)
        googlesearchconsole_last_16m_page_export.collect()
