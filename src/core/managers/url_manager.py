import logging
from urllib.parse import urlparse, urlunparse
from core.models.url import Url

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
        return parsed_url.path + ('?' + parsed_url.query if parsed_url.query else '')
