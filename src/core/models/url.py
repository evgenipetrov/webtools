import logging
from urllib.parse import urlparse, urlunparse

from django.db import models

from core.models.website import Website


class Url(models.Model):
    # relations
    website = models.ForeignKey(Website, on_delete=models.CASCADE)  # from project website

    # primary attributes
    full_address = models.URLField(max_length=2048, unique=True)  # from screamingfrog list crawl
    status_code = models.IntegerField(null=True, blank=True)  # from screamingfrog list crawl
    redirect_url = models.URLField(max_length=2048, null=True, blank=True)  # from screamingfrog list crawl

    # secondary attributes
    relative_address = models.CharField(max_length=2048, null=True, blank=True)  # calculate
    content_type = models.CharField(max_length=2048, null=True, blank=True)  # from screamingfrog list crawl
    indexability = models.CharField(max_length=2048, null=True, blank=True)  # from screamingfrog list crawl
    indexability_status = models.CharField(max_length=2048, null=True, blank=True)  # from screamingfrog list crawl
    title_1 = models.CharField(max_length=2048, null=True, blank=True)  # from screamingfrog list crawl
    meta_description_1 = models.CharField(max_length=2048, null=True, blank=True)  # from screamingfrog list crawl
    h1_1 = models.CharField(max_length=2048, null=True, blank=True)  # from screamingfrog list crawl
    canonical_link_element_1 = models.URLField(max_length=2048, null=True, blank=True)  # from screamingfrog list crawl
    word_count = models.IntegerField(null=True, blank=True)  # from screamingfrog list crawl
    readability = models.CharField(max_length=2048, null=True, blank=True)  # from screamingfrog list crawl
    crawl_depth = models.IntegerField(null=True, blank=True)  # from screamingfrog spider crawl
    unique_inlinks = models.IntegerField(null=True, blank=True)  # from screamingfrog list crawl
    unique_outlinks = models.IntegerField(null=True, blank=True)  # from screamingfrog list crawl
    hash = models.CharField(max_length=2048, null=True, blank=True)  # from screamingfrog list crawl
    crawl_timestamp = models.DateTimeField(null=True, blank=True)  # from screamingfrog list crawl
    in_sitemap = models.BooleanField(default=False)  # from screamingfrog sitemap crawl
    html_template = models.CharField(max_length=2048, null=True, blank=True)  # from sitebulb list crawl
    content_words_count = models.IntegerField(null=True, blank=True)  # from sitebulb list crawl
    template_words_count = models.IntegerField(null=True, blank=True)  # from sitebulb list crawl
    word_count2 = models.IntegerField(null=True, blank=True)  # from sitebulb list crawl

    # system attributes
    created_at = models.DateTimeField(auto_now_add=True)  # auto
    updated_at = models.DateTimeField(auto_now=True)  # auto

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
            logger.info(f"URL instance does not exist - creating: '{full_address}'")
        else:
            logger.info(f"URL instance already exists - updating: '{full_address}'")
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

    @staticmethod
    def get_url_by_website(website):
        urls = Url.objects.filter(website=website)
        return urls
