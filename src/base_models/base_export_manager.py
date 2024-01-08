import logging
import os
import shutil

import pandas as pd
from pandas.errors import EmptyDataError

from core.models import Project

logger = logging.getLogger(__name__)


class BaseExportManager:
    def __init__(self, project: Project, export_subfolder):
        self.force = False
        self.project = project
        self.export_subfolder = export_subfolder
        self.export_path = os.path.join(project.data_folder, self.export_subfolder)

    def empty_export_folder(self):
        if not os.path.exists(self.export_path):
            os.makedirs(self.export_path)
        for filename in os.listdir(self.export_path):
            file_path = os.path.join(self.export_path, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

    def run(self, force=False):
        """
        Template method that defines the export collection workflow.
        If 'force' is True, skips user confirmation.
        """
        self.force = force

        if not self.force:
            self.confirm_export(self.export_subfolder)

        if self.force:
            self.empty_export_folder()
            self.perform_pre_export_action()
            self.perform_export()
            self.perform_post_export_action()
        else:
            logger.info("Export process aborted.")

    def confirm_export(self, export_description):
        """
        Asks the user whether to proceed with a specific export process.
        Proceeds if the user inputs 'y' or presses Enter without input.
        """
        response = input(f"Do you want to proceed with {export_description.upper()} export? [y/N]: ").strip().lower()
        self.force = response == "y"

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
        if os.path.exists(self.export_path):
            for filename in os.listdir(self.export_path):
                if filename.endswith(".csv"):
                    file_path = os.path.join(self.export_path, filename)
                    try:
                        df = pd.read_csv(file_path)
                        all_dataframes.append(df)
                    except EmptyDataError:
                        # Skip file if it is empty or contains no parseable data
                        continue
        else:
            print(f"Directory not found: {self.export_path}")

        # Concatenate all dataframes if there are any, else return an empty dataframe
        if all_dataframes:
            return pd.concat(all_dataframes, ignore_index=True)
        else:
            return pd.DataFrame()
