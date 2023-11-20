from core.models.url import Url
from urllib.parse import urlparse, urlunparse


class UrlManager:
    @staticmethod
    def get_hostname(full_address):
        """
        Extract the hostname from a given URL.
        """
        parsed_url = urlparse(full_address)
        return parsed_url.hostname

    @staticmethod
    def remove_url_fragment(full_address):
        """
        Remove the fragment from a URL.
        """
        parsed_url = urlparse(full_address)
        # Reconstruct the URL without the fragment
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
        return clean_url

    @staticmethod
    def get_root_url(full_address):
        """
        Return root URL address of a website.
        """
        parsed_url = urlparse(full_address)
        # Reconstruct the URL
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
        return root_url

    @staticmethod
    def create_url(full_address, **kwargs):
        """
        Create a new url instance.
        """
        url, created = Url.objects.get_or_create(full_address=full_address, defaults=kwargs)
        return url

    @staticmethod
    def update_url(full_address, **kwargs):
        """
        Updates or creates a URL instance with given kwargs.
        """
        url, created = Url.objects.update_or_create(full_address=full_address, defaults=kwargs)
        return url
