import logging

from django.db import models

from core.models.keyword import Keyword
from core.models.url import Url
from core.models.website import Website

logger = logging.getLogger(__name__)


class Ranking(models.Model):
    # primary attributes
    keyword = models.ForeignKey(Keyword, on_delete=models.CASCADE)
    website = models.ForeignKey(Website, on_delete=models.CASCADE)

    semrush_url = models.ForeignKey(Url, on_delete=models.CASCADE, null=True, blank=True)
    semrush_current_position = models.PositiveSmallIntegerField(null=True, blank=True)
    semrush_previous_position = models.PositiveSmallIntegerField(null=True, blank=True)
    semrush_timestamp = models.DateField(null=True, blank=True)
    semrush_position_type = models.CharField(max_length=255, null=True, blank=True)

    # system attributes
    created_at = models.DateTimeField(auto_now_add=True)  # auto
    updated_at = models.DateTimeField(auto_now=True)  # auto

    def __str__(self):
        return f"{self.keyword.phrase} / {self.url.full_address}"

    class Meta:
        verbose_name = "Ranking"
        verbose_name_plural = "Rankings"

        unique_together = ("website", "keyword")


class RankingManager:
    @staticmethod
    def push_ranking(keyword, website, **kwargs):
        ranking, created = Ranking.objects.update_or_create(keyword=keyword, website=website, defaults=kwargs)
        if created:
            logger.debug(f"RANKING instance does not exist - creating: '{keyword.phrase}' / {website.root_url}")
        else:
            logger.debug(f"RANKING instance already exists - updating: '{keyword.phrase}' / {website.root_url}")
        return ranking
