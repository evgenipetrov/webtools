from django.db import models
import logging

logger = logging.getLogger(__name__)


class Project(models.Model):
    name = models.CharField(max_length=255, unique=True)
    base_url = models.URLField(max_length=200)
    data_folder = models.CharField(max_length=255)

    gsc_auth_domain = models.CharField(max_length=255, blank=True, null=True)
    gsc_property_name = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return f"Project {self.id} - {self.base_url}"

    class Meta:
        verbose_name = "Project"
        verbose_name_plural = "Projects"
        unique_together = (
            "base_url",
            "data_folder",
        )


class ProjectManager:
    @staticmethod
    def create_project_by_name(name: str):
        logger.info(f"Project '{name}' not found. Creating new project.")
        # Initialize a new Project instance
        project = Project()

        # Set the name of the project
        project.name = name

        # Iterate through the other fields and prompt for input
        for field in Project._meta.get_fields():
            # Skip the 'id' field, relational fields, and already set fields
            if field.name in ['id', 'name'] or isinstance(field, (
            models.ManyToOneRel, models.ManyToManyField, models.ManyToOneRel)):
                continue

            # Prompt for input based on field type
            user_input = input(f"Enter {field.name} ({'optional' if field.blank else 'required'}): ")

            # Handle nullable and optional fields
            if user_input or not field.blank:
                # Convert input to the appropriate type if necessary (e.g. for URLField)
                if isinstance(field, models.URLField):
                    # Add URL validation or conversion here if necessary
                    setattr(project, field.name, user_input)
                else:
                    setattr(project, field.name, user_input)

        # Save the new project instance
        project.save()
        logger.info(f"Created project: '{name}'.")
        return project

    @staticmethod
    def get_project_by_id(project_id):
        return Project.objects.get(id=project_id)

    @staticmethod
    def get_project_by_name(name: str):
        return Project.objects.filter(name=name).first()
