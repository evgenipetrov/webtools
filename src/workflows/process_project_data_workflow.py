import pandas as pd

from exports.googlesearchconsole_last_16m_page_export import GoogleSearchConsoleLast16mPageExport
from exports.screamingfrog_sitemap_crawl_export import ScreamingFrogSitemapCrawlExport
from exports.screamingfrog_spider_crawl_export import ScreamingFrogSpiderCrawlExport
from exports.sitebulb_url_internal_export import SitebulbUrlInternalExport
from core.managers.project_manager import ProjectManager


class ProcessProjectDataWorkflow:
    def __init__(self, project):
        self.project_id = project.id

    def execute(self):
        project = ProjectManager.get_project(project_id=self.project_id)

        # first we need to update URL list:
        screamingfrog_sitemap_crawl_export = ScreamingFrogSitemapCrawlExport(project)
        screamingfrog_sitemap_crawl_data = screamingfrog_sitemap_crawl_export.get_data()

        screamingfrog_spider_crawl_export = ScreamingFrogSpiderCrawlExport(project)
        screamingfrog_spider_crawl_data = screamingfrog_spider_crawl_export.get_data()

        sitebulb_url_internal_export = SitebulbUrlInternalExport(project)
        sitebulb_url_internal_data = sitebulb_url_internal_export.get_data()

        googlesearchconsole_last_16m_page_export = GoogleSearchConsoleLast16mPageExport(project)
        googlesearchconsole_last_16m_page_data = googlesearchconsole_last_16m_page_export.get_data()

        sitemap_urls = screamingfrog_sitemap_crawl_data["Address"]
        spider_urls = screamingfrog_spider_crawl_data["Address"]
        sitebulb_urls = sitebulb_url_internal_data["URL"]
        gsc_urls = googlesearchconsole_last_16m_page_data["page"]

        # Combine all URL data into a single Series
        all_urls = pd.concat([sitemap_urls, spider_urls, sitebulb_urls, gsc_urls], axis=0)

        # Extract unique URLs
        unique_urls = pd.Series(all_urls.unique())

        a = 1
