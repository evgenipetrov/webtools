from core.models.project import Project


class ProjectManager:
    @staticmethod
    def create_project(name, base_url, data_folder):
        """
        Create a new project instance.
        """
        project = Project(name=name, base_url=base_url, data_folder=data_folder)
        project.save()
        return project
