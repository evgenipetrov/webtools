import logging

from django.db import models

from core.models.keyword import Keyword
from core.models.url import Url
from core.models.website import Website

logger = logging.getLogger(__name__)


class UrlRanking(models.Model):
    # primary attributes
    url = models.ForeignKey(Url, on_delete=models.CASCADE)
    website = models.ForeignKey(Website, on_delete=models.CASCADE)

    semrush_keyword_best_by_volume = models.ForeignKey(Keyword, on_delete=models.CASCADE, null=True, blank=True, related_name="semrush_keyword_best_by_volume_urlrankings")
    semrush_current_position_best_by_volume = models.PositiveSmallIntegerField(null=True, blank=True)
    semrush_previous_position_best_by_volume = models.PositiveSmallIntegerField(null=True, blank=True)
    semrush_timestamp_best_by_volume = models.DateField(null=True, blank=True)
    semrush_position_type_best_by_volume = models.CharField(max_length=255, null=True, blank=True)

    semrush_keyword_best_by_position = models.ForeignKey(Keyword, on_delete=models.CASCADE, null=True, blank=True, related_name="semrush_keyword_best_by_position_urlrankings")
    semrush_current_position_best_by_position = models.PositiveSmallIntegerField(null=True, blank=True)
    semrush_previous_position_best_by_position = models.PositiveSmallIntegerField(null=True, blank=True)
    semrush_timestamp_best_by_position = models.DateField(null=True, blank=True)
    semrush_position_type_best_by_position = models.CharField(max_length=255, null=True, blank=True)
    # last_1m
    gsc_query_last_1m_best_by_clicks = models.ForeignKey(Keyword, on_delete=models.CASCADE, null=True, blank=True, related_name="gsc_query_last_1m_best_by_clicks_urlrankings")
    gsc_impressions_last_1m_best_by_clicks = models.IntegerField(null=True, blank=True)
    gsc_clicks_last_1m_best_by_clicks = models.IntegerField(null=True, blank=True)
    gsc_ctr_last_1m_best_by_clicks = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    gsc_position_last_1m_best_by_clicks = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)

    gsc_query_last_1m_best_by_impressions = models.ForeignKey(Keyword, on_delete=models.CASCADE, null=True, blank=True, related_name="gsc_query_last_1m_best_by_impressions_urlrankings")
    gsc_impressions_last_1m_best_by_impressions = models.IntegerField(null=True, blank=True)
    gsc_clicks_last_1m_best_by_impressions = models.IntegerField(null=True, blank=True)
    gsc_ctr_last_1m_best_by_impressions = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    gsc_position_last_1m_best_by_impressions = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)

    gsc_query_last_1m_best_by_position = models.ForeignKey(Keyword, on_delete=models.CASCADE, null=True, blank=True, related_name="gsc_query_last_1m_best_by_position_urlrankings")
    gsc_impressions_last_1m_best_by_position = models.IntegerField(null=True, blank=True)
    gsc_clicks_last_1m_best_by_position = models.IntegerField(null=True, blank=True)
    gsc_ctr_last_1m_best_by_position = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    gsc_position_last_1m_best_by_position = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    # end
    # previous_1m
    gsc_query_previous_1m_best_by_clicks = models.ForeignKey(Keyword, on_delete=models.CASCADE, null=True, blank=True, related_name="gsc_query_previous_1m_best_by_clicks_urlrankings")
    gsc_impressions_previous_1m_best_by_clicks = models.IntegerField(null=True, blank=True)
    gsc_clicks_previous_1m_best_by_clicks = models.IntegerField(null=True, blank=True)
    gsc_ctr_previous_1m_best_by_clicks = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    gsc_position_previous_1m_best_by_clicks = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)

    gsc_query_previous_1m_best_by_impressions = models.ForeignKey(Keyword, on_delete=models.CASCADE, null=True, blank=True, related_name="gsc_query_previous_1m_best_by_impressions_urlrankings")
    gsc_impressions_previous_1m_best_by_impressions = models.IntegerField(null=True, blank=True)
    gsc_clicks_previous_1m_best_by_impressions = models.IntegerField(null=True, blank=True)
    gsc_ctr_previous_1m_best_by_impressions = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    gsc_position_previous_1m_best_by_impressions = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)

    gsc_query_previous_1m_best_by_position = models.ForeignKey(Keyword, on_delete=models.CASCADE, null=True, blank=True, related_name="gsc_query_previous_1m_best_by_position_urlrankings")
    gsc_impressions_previous_1m_best_by_position = models.IntegerField(null=True, blank=True)
    gsc_clicks_previous_1m_best_by_position = models.IntegerField(null=True, blank=True)
    gsc_ctr_previous_1m_best_by_position = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    gsc_position_previous_1m_best_by_position = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    # end
    # last_1m_previous_year
    gsc_query_last_1m_previous_year_best_by_clicks = models.ForeignKey(Keyword, on_delete=models.CASCADE, null=True, blank=True, related_name="gsc_query_last_1m_previous_year_best_by_clicks_urlrankings")
    gsc_impressions_last_1m_previous_year_best_by_clicks = models.IntegerField(null=True, blank=True)
    gsc_clicks_last_1m_previous_year_best_by_clicks = models.IntegerField(null=True, blank=True)
    gsc_ctr_last_1m_previous_year_best_by_clicks = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    gsc_position_last_1m_previous_year_best_by_clicks = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)

    gsc_query_last_1m_previous_year_best_by_impressions = models.ForeignKey(Keyword, on_delete=models.CASCADE, null=True, blank=True, related_name="gsc_query_last_1m_previous_year_best_by_impressions_urlrankings")
    gsc_impressions_last_1m_previous_year_best_by_impressions = models.IntegerField(null=True, blank=True)
    gsc_clicks_last_1m_previous_year_best_by_impressions = models.IntegerField(null=True, blank=True)
    gsc_ctr_last_1m_previous_year_best_by_impressions = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    gsc_position_last_1m_previous_year_best_by_impressions = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)

    gsc_query_last_1m_previous_year_best_by_position = models.ForeignKey(Keyword, on_delete=models.CASCADE, null=True, blank=True, related_name="gsc_query_last_1m_previous_year_best_by_position_urlrankings")
    gsc_impressions_last_1m_previous_year_best_by_position = models.IntegerField(null=True, blank=True)
    gsc_clicks_last_1m_previous_year_best_by_position = models.IntegerField(null=True, blank=True)
    gsc_ctr_last_1m_previous_year_best_by_position = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    gsc_position_last_1m_previous_year_best_by_position = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    # end
    # last_16m
    gsc_query_last_16m_best_by_clicks = models.ForeignKey(Keyword, on_delete=models.CASCADE, null=True, blank=True, related_name="gsc_query_last_16m_best_by_clicks_urlrankings")
    gsc_impressions_last_16m_best_by_clicks = models.IntegerField(null=True, blank=True)
    gsc_clicks_last_16m_best_by_clicks = models.IntegerField(null=True, blank=True)
    gsc_ctr_last_16m_best_by_clicks = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    gsc_position_last_16m_best_by_clicks = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)

    gsc_query_last_16m_best_by_impressions = models.ForeignKey(Keyword, on_delete=models.CASCADE, null=True, blank=True, related_name="gsc_query_last_16m_best_by_impressions_urlrankings")
    gsc_impressions_last_16m_best_by_impressions = models.IntegerField(null=True, blank=True)
    gsc_clicks_last_16m_best_by_impressions = models.IntegerField(null=True, blank=True)
    gsc_ctr_last_16m_best_by_impressions = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    gsc_position_last_16m_best_by_impressions = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)

    gsc_query_last_16m_best_by_position = models.ForeignKey(Keyword, on_delete=models.CASCADE, null=True, blank=True, related_name="gsc_query_last_16m_best_by_position_urlrankings")
    gsc_impressions_last_16m_best_by_position = models.IntegerField(null=True, blank=True)
    gsc_clicks_last_16m_best_by_position = models.IntegerField(null=True, blank=True)
    gsc_ctr_last_16m_best_by_position = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    gsc_position_last_16m_best_by_position = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    # end

    # system attributes
    created_at = models.DateTimeField(auto_now_add=True)  # auto
    updated_at = models.DateTimeField(auto_now=True)  # auto

    def __str__(self):
        return f"{self.url.full_address}"

    class Meta:
        verbose_name = "UrlRanking"
        verbose_name_plural = "UrlRankings"

        unique_together = ("website", "url")


class UrlRankingManager:
    @staticmethod
    def push_ranking(url, website, **kwargs):
        urlranking, created = UrlRanking.objects.update_or_create(url=url, website=website, defaults=kwargs)
        if created:
            logger.debug(f"RANKING instance does not exist - creating: '{urlranking}'")
        else:
            logger.debug(f"RANKING instance already exists - updating: '{urlranking}'")
        return urlranking
