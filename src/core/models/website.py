from django.db import models

from core.models import Project


class Website(models.Model):
    root_url = models.URLField(max_length=2048, unique=True)

    project = models.ForeignKey(Project, on_delete=models.CASCADE, null=True)

    def __str__(self):
        return self.root_url

    class Meta:
        verbose_name = "Website"
        verbose_name_plural = "Websites"


class WebsiteManager:
    @staticmethod
    def create_website(root_url, project=None):
        """
        Create a new website instance.
        """
        website, _ = Website.objects.get_or_create(root_url=root_url, project=project)
        return website

    @staticmethod
    def get_website(website_id):
        """
        Retrieve a website instance by its ID.
        """
        try:
            return Website.objects.get(id=website_id)
        except Website.DoesNotExist:
            return None

    @staticmethod
    def get_website_by_project(project):
        try:
            website = Website.objects.get(project=project)
            return website
        except Website.DoesNotExist:
            print("No website found for this project.")
        except Website.MultipleObjectsReturned:
            print("Multiple websites found for this project.")
