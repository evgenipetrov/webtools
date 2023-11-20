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
