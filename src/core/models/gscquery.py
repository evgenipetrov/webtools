import logging

from django.db import models

from core.models.keyword import Keyword, KeywordManager
from core.models.website import Website

logger = logging.getLogger(__name__)


class GscQuery(models.Model):
    # relations
    website = models.ForeignKey(Website, on_delete=models.CASCADE)

    # primary attributes
    keyword = models.ForeignKey(Keyword, on_delete=models.CASCADE)

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
        return self.keyword.phrase

    class Meta:
        verbose_name = "GscQuery"
        verbose_name_plural = "GscQueries"

        unique_together = ("website", "keyword")


class GscQueryManager:
    @staticmethod
    def push_gscquery(term, website, **kwargs):
        """
        Pushes query instance to gsc query entries.
        """
        keyword = KeywordManager.push_keyword(term)
        gscquery, created = GscQuery.objects.update_or_create(keyword=keyword, website=website, defaults=kwargs)
        if created:
            logger.debug(f"GSCQUERY instance does not exist - creating: '{term}'")
        else:
            logger.debug(f"GSCQUERY instance already exists - updating: '{term}'")
        return gscquery

    @staticmethod
    def get_gscqueries_by_website(website):
        objects = GscQuery.objects.filter(website=website)
        return objects
