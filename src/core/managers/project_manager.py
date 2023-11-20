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

    @staticmethod
    def get_project(project_id):
        """
        Retrieve a project instance by its ID.
        """
        try:
            return Project.objects.get(id=project_id)
        except Project.DoesNotExist:
            return None

    @staticmethod
    def get_project_by_name(name):
        """
        Retrieve a project instance by its name.
        """
        try:
            return Project.objects.get(name=name)
        except Project.DoesNotExist:
            return None
