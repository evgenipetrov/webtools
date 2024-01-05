import os
import shutil
import subprocess
import tempfile

import docker

from django.conf import settings


class ScreamingFrogSeoSpiderService:
    def __init__(self):
        self.client = docker.from_env()
        self.image = "screamingfrogseospider"
        self.parameters = ["--headless", "--overwrite"]
        self.container = None

        # Ensure temp directory exists
        os.makedirs(settings.TEMP_DIR, exist_ok=True)

    def set_crawl_config(self, seospiderconfig):
        self.parameters.append(f"--config {seospiderconfig}")
        return self

    def set_crawl_url(self, url):
        self.parameters.append(f"--crawl {url}")
        return self

    def set_sitemap_url(self, sitemap_url):
        self.parameters.append(f"--crawl-sitemap {sitemap_url}")
        return self

    def set_export_tabs(self, export_tabs):
        self.parameters.append(f"--export-tabs {export_tabs}")
        return self

    @staticmethod
    def _empty_temp_dir():
        for filename in os.listdir(settings.TEMP_DIR):
            file_path = os.path.join(settings.TEMP_DIR, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

    def run(self):
        self._empty_temp_dir()  # Empty the TEMP_DIR
        volumes = {settings.TEMP_DIR: {"bind": "/export", "mode": "rw"}}
        self.container = self.client.containers.run(self.image, " ".join(self.parameters), volumes=volumes, detach=True, auto_remove=True)
        for line in self.container.logs(stream=True):
            print(line.strip().decode())
