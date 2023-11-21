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
    def create_url(full_address, **kwargs):
        """
        Create a new url instance.
        """
        url, created = Url.objects.get_or_create(full_address=full_address, defaults=kwargs)
        if created:
            logger.info(f"Created new URL instance for address '{full_address}'.")
        else:
            logger.debug(f"URL instance for address '{full_address}' already exists.")
        return url

    @staticmethod
    def update_url(full_address, **kwargs):
        """
        Updates or creates a URL instance with given kwargs.
        """
        url, created = Url.objects.update_or_create(full_address=full_address, defaults=kwargs)
        if created:
            logger.info(f"Created new URL instance for address '{full_address}'.")
        else:
            logger.info(f"Updated URL instance for address '{full_address}'.")
        return url
