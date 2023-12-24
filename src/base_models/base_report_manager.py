import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)


class BaseReportManager:
    def __init__(self, project, report_subfolder="_reports"):
        self._project = project
        self._report_filename = None
        self._report_columns = None
        self._report_data = None
        self._report_subfolder = report_subfolder
        self._report_path = os.path.join(project.data_folder, self._report_subfolder)
        self._ensure_report_path()

    def _ensure_report_path(self):
        """
        Ensures that the report folder exists.
        """
        if not os.path.exists(self._report_path):
            os.makedirs(self._report_path)

    def run(self):
        """
        Template method that defines the report generation workflow.
        """
        self.prepare_report_base()
        self.generate_report()
        self.save_report()

    def prepare_report_base(self):
        """
        Prepare the environment before generating the report.
        Must be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def generate_report(self):
        """
        The main report generation logic.
        Must be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def save_report(self):
        """
        Converts the report data to a DataFrame and saves it as a CSV file.
        """
        df = pd.DataFrame(self._report_data, columns=self._report_columns)
        report_file_path = os.path.join(self._report_path, self._report_filename)
        df.to_csv(report_file_path, index=False)
