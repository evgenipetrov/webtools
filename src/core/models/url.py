from django.db import models

from core.models.website import Website


class Url(models.Model):
    full_address = models.URLField(max_length=2048, unique=True)
    status_code = models.IntegerField(null=True, blank=True)
    redirect_url = models.URLField(max_length=2048, null=True, blank=True)

    website = models.ForeignKey(Website, on_delete=models.CASCADE)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.full_address

    class Meta:
        verbose_name = "Url"
        verbose_name_plural = "Urls"
