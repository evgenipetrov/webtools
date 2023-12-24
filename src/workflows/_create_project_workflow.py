import logging

from core.models.project import ProjectManager
from core.models.url import UrlManager
from core.models.website import WebsiteManager

logger = logging.getLogger(__name__)


class CreateProjectWorkflow:
    def __init__(self, project_name, project_url, project_data_folder):
        self.project_name = project_name
        self.project_url = project_url
        self.project_data_folder = project_data_folder

    def execute(self):
        root_url = UrlManager.get_root_url(self.project_url)

        project = ProjectManager.create_project(
            name=self.project_name,
            base_url=root_url,
            data_folder=self.project_data_folder,
        )

        website = WebsiteManager.create_website(root_url=root_url, project=project)
        url = UrlManager.push_url(full_address=root_url, website=website)
