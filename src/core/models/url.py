import logging
from urllib.parse import urlparse, urlunparse

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


logger = logging.getLogger(__name__)


class UrlManager:
    @staticmethod
    def get_hostname(full_address):
        """
        Extract the hostname from a given URL.
        """
        parsed_url = urlparse(full_address)
        hostname = parsed_url.hostname
        logger.debug(f"Extracted hostname '{hostname}' from URL '{full_address}'.")
        return hostname

    @staticmethod
    def remove_url_fragment(full_address):
        """
        Remove the fragment from a URL.
        """
        parsed_url = urlparse(full_address)
        clean_url = urlunparse(
            (
                parsed_url.scheme,
                parsed_url.netloc,
                parsed_url.path,
                parsed_url.params,
                parsed_url.query,
                "",
            )
        )
        logger.debug(f"Removed fragment from URL '{full_address}'. New URL: '{clean_url}'.")
        return clean_url

    @staticmethod
    def get_root_url(full_address):
        """
        Return root URL address of a website.
        """
        parsed_url = urlparse(full_address)
        root_url = urlunparse(
            (
                parsed_url.scheme,
                parsed_url.netloc,
                "",
                "",
                "",
                "",
            )
        )
        logger.debug(f"Extracted root URL '{root_url}' from URL '{full_address}'.")
        return root_url

    @staticmethod
    def push_url(full_address, website, **kwargs):
        """
        Pushes url instance to website.
        """
        url, create = Url.objects.update_or_create(full_address=full_address, website=website, defaults=kwargs)
        if create:
            logger.info(f"Created new URL instance for address '{full_address}'.")
        else:
            logger.info(f"URL instance for address '{full_address}' already exists.")
        return url

    @staticmethod
    def get_urls_by_website(website):
        """
        Returns all URLs for a website.
        """
        urls = Url.objects.filter(website=website)
        return urls

    @staticmethod
    def get_relative_url(full_url):
        parsed_url = urlparse(full_url)
        return parsed_url.path + ("?" + parsed_url.query if parsed_url.query else "")

    @staticmethod
    def build_full_address(root_url, path):
        parsed_url = urlparse(root_url)
        return urlunparse((parsed_url.scheme, parsed_url.netloc, path, "", "", ""))
