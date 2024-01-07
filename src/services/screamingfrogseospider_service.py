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

    def set_crawl_list(self, listfile):
        self.parameters.append(f"--crawl-list {listfile}")
        return self

    def set_export_tabs(self, export_tabs):
        self.parameters.append(f"--export-tabs {export_tabs}")
        return self

    def run(self):
        volumes = {settings.TEMP_DIR: {"bind": "/export", "mode": "rw"}}
        self.container = self.client.containers.run(self.image, " ".join(self.parameters), volumes=volumes, detach=True, auto_remove=True)
        for line in self.container.logs(stream=True):
            print(line.strip().decode())
