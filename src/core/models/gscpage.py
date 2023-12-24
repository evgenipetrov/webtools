import logging

from django.db import models

from core.models.website import Website

logger = logging.getLogger(__name__)


class GscPage(models.Model):
    # relations
    website = models.ForeignKey(Website, on_delete=models.CASCADE)

    # primary attributes
    full_address = models.URLField(max_length=2048, unique=True)
    status_code = models.IntegerField(null=True, blank=True)
    redirect_url = models.URLField(max_length=2048, null=True, blank=True)

    impressions_last_1m = models.IntegerField(null=True, blank=True)
    impressions_last_16m = models.IntegerField(null=True, blank=True)
    impressions_last_1m_previous_year = models.IntegerField(null=True, blank=True)
    impressions_previous_1m = models.IntegerField(null=True, blank=True)

    clicks_last_1m = models.IntegerField(null=True, blank=True)
    clicks_last_16m = models.IntegerField(null=True, blank=True)
    clicks_last_1m_previous_year = models.IntegerField(null=True, blank=True)
    clicks_previous_1m = models.IntegerField(null=True, blank=True)

    ctr_last_1m = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    ctr_last_16m = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    ctr_last_1m_previous_year = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    ctr_previous_1m = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)

    position_last_1m = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    position_last_16m = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    position_last_1m_previous_year = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    position_previous_1m = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)

    # system attributes
    created_at = models.DateTimeField(auto_now_add=True)  # auto
    updated_at = models.DateTimeField(auto_now=True)  # auto

    def __str__(self):
        return self.full_address

    class Meta:
        verbose_name = "GscPage"
        verbose_name_plural = "GscPages"


class GscManager:
    @staticmethod
    def push_gscpage(full_address, website, **kwargs):
        """
        Pushes page instance to gsc page entries.
        """
        gscpage, create = GscPage.objects.update_or_create(full_address=full_address, website=website, defaults=kwargs)
        if create:
            logger.info(f"GSCPAGE instance does not exist - creating: '{full_address}'")
        else:
            logger.info(f"GSCPAGE instance already exists - updating: '{full_address}'")
        return gscpage

    @staticmethod
    def get_gscpages_by_website(website):
        objects = GscPage.objects.filter(website=website)
        return objects
