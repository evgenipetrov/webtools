from django.db import models


class Project(models.Model):
    name = models.CharField(max_length=255, unique=True)
    base_url = models.URLField(max_length=200)
    data_folder = models.CharField(max_length=255)

    gsc_auth_domain = models.CharField(max_length=255, blank=True, null=True)
    gsc_property_name = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return f"Project {self.id} - {self.base_url}"

    class Meta:
        verbose_name = "Project"
        verbose_name_plural = "Projects"
        unique_together = (
            "base_url",
            "data_folder",
        )
