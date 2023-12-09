import os

from core.models import Project


class BaseReport:
    def __init__(self, project: Project):
        self.file_name = None
        self.project = project

    def fetch_data(self):
        """
        Fetch data required for the report.
        This method should be overridden by subclasses to retrieve specific data.
        """
        raise NotImplementedError("fetch_data method must be implemented in subclasses")

    def process_data(self):
        """
        Process the fetched data.
        This method can be overridden by subclasses for specific data processing needs.
        """
        raise NotImplementedError("process_data method must be implemented in subclasses")

    def generate_report(self):
        """
        Generate the report based on fetched and processed data.
        This method should be overridden by subclasses to generate specific report types.
        """
        raise NotImplementedError("generate_report method must be implemented in subclasses")

    def save_report(self, report):
        """
        Save the report to a file.
        This method can be overridden by subclasses if they need a specific saving mechanism.
        """
        reports_folder = os.path.join(self.project.data_folder, "_reports")
        os.makedirs(reports_folder, exist_ok=True)  # Create _reports folder if it doesn't exist
        file_path = os.path.join(reports_folder, self.file_name)
        report.to_csv(file_path)

    def run(self):
        """
        The main method to run the report generation process.
        """
        self.fetch_data()
        self.process_data()
        report = self.generate_report()
        self.save_report(report)
