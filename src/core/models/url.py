from django.db import models

from core.models.website import Website


class Url(models.Model):
    full_address = models.URLField(max_length=2048, unique=True)

    website = models.ForeignKey(Website, on_delete=models.CASCADE)

    def __str__(self):
        return self.full_address

    class Meta:
        verbose_name = "Url"
        verbose_name_plural = "Urls"