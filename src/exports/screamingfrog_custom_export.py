import logging

import pandas as pd

from base_models.base_export_manager import BaseExportManager
from core.models.url import UrlManager
from core.models.website import WebsiteManager

logger = logging.getLogger(__name__)
EXPORT_SUBFOLDER = "screamingfrog_custom_export"


class ScreamingFrogCustomExport(BaseExportManager):
    def __init__(self, project):
        super().__init__(project, EXPORT_SUBFOLDER)

    def perform_pre_export_action(self):
        """
        Provides instructions for Screaming Frog list crawl export.
        """
        website = WebsiteManager.get_website_by_project(self.project)
        url_objects = UrlManager.get_urls_by_website(website)
        urls_df = pd.DataFrame([url_object.full_address for url_object in url_objects], columns=["full_address"])

        urls_df.to_clipboard(index=False, header=False)

        print(f"The urls to crawl are copied to clipboard.")
        print(f"Please export the Screaming Frog list crawl data as CSV. Verify for 429 error codes.")
        print(f"Place the exported file(s) in the following directory: {self.export_path}")

    def perform_export(self):
        """
        Implement the actual export logic here.
        """
        # Export logic or automated steps specific to Screaming Frog
        pass

    def perform_post_export_action(self):
        """
        Any post-export actions.
        """
        input("Press ENTER to continue after placing the exported files.")
        df = self.get_data()
        # clean urls
        df = df[~df["Address"].str.contains("#")]  # remove fragments
        # process urls
        website = WebsiteManager.get_website_by_project(self.project)

        for index, row in df.iterrows():
            # Construct kwargs dictionary for each row
            kwargs = {"status_code": row["Status Code"], "redirect_url": row["Redirect URL"]}
            # Pass kwargs with unpacking operator '**'
            UrlManager.push_url(full_address=row["Address"], website=website, **kwargs)
