from core.models.website import Website


class WebsiteManager:
    @staticmethod
    def create_website(root_url, project=None):
        """
        Create a new website instance.
        """
        website, _ = Website.objects.get_or_create(root_url=root_url, project=project)
        return website
