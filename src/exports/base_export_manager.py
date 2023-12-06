import os

import pandas as pd
import logging

from pandas.errors import EmptyDataError

logger = logging.getLogger(__name__)


class BaseExportManager:
    def __init__(self, project, export_subfolder):
        self.project = project
        self.export_subfolder = export_subfolder
        self.export_path = os.path.join(project.data_folder, self.export_subfolder)
        self.ensure_export_path()

    def ensure_export_path(self):
        """
        Ensures that the export folder exists.
        """
        if not os.path.exists(self.export_path):
            os.makedirs(self.export_path)

    def run(self):
        """
        Template method that defines the export collection workflow.
        """
        # Ask user for confirmation before proceeding
        if self.confirm_export(self.export_subfolder):
            self.perform_pre_export_action()
            self.perform_export()
            self.perform_post_export_action()

    @staticmethod
    def confirm_export(export_description):
        """
        Asks the user whether to proceed with a specific export process.
        """
        default_response = "n"  # Default to no
        response = input(f"Do you want to proceed with {export_description} export? (y/n) [{default_response}]: ")

        if response.strip().lower() not in ("y", "n"):
            return False

        if response.strip().lower() == "y":
            return True
        else:
            return False

    def perform_pre_export_action(self):
        """
        Perform any actions that need to happen before the export process starts.
        Must be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def perform_export(self):
        """
        The main export logic.
        Must be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def perform_post_export_action(self):
        """
        Perform any actions that need to happen after the export process is completed.
        Must be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def get_data(self):
        """
        Reads all CSV files in the export folder into a single DataFrame.
        """
        all_dataframes = []
        for filename in os.listdir(self.export_path):
            if filename.endswith(".csv"):
                file_path = os.path.join(self.export_path, filename)
                try:
                    df = pd.read_csv(file_path)
                    all_dataframes.append(df)
                except EmptyDataError:
                    # Skip file if it is empty or contains no parseable data
                    continue

        # Concatenate all dataframes if there are any, else return an empty dataframe
        if all_dataframes:
            return pd.concat(all_dataframes, ignore_index=True)
        else:
            return pd.DataFrame()
