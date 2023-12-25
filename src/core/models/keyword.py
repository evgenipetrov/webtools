import logging

from django.db import models

from core.models.website import Website

logger = logging.getLogger(__name__)


class Keyword(models.Model):
    # primary attributes
    phrase = models.CharField(max_length=255, unique=True)

    volume = models.IntegerField(default=0)

    # system attributes
    created_at = models.DateTimeField(auto_now_add=True)  # auto
    updated_at = models.DateTimeField(auto_now=True)  # auto

    def __str__(self):
        return self.phrase

    class Meta:
        verbose_name = "Keyword"
        verbose_name_plural = "Keywords"


class KeywordManager:
    @staticmethod
    def push_keyword(phrase, **kwargs):
        keyword, created = Keyword.objects.update_or_create(phrase=phrase, defaults=kwargs)
        if created:
            logger.debug(f"KEYWORD instance does not exist - creating: '{phrase}'")
        else:
            logger.debug(f"KEYWORD instance already exists - updating: '{phrase}'")
        return keyword
