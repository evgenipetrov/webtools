# webtools

## Overview
This project is designed to manage and track vital information about web pages across various websites. It serves as a centralized platform for storing and accessing data such as URLs, rankings, keywords, search volumes, and other related metrics. Primarily aimed at webmasters, SEO specialists, and digital marketers, this tool offers an efficient way to monitor and analyze the performance of web pages, aiding in data-driven decision-making to enhance online visibility and performance.

## Project Structure
This project adopts a modular architecture with separate Django apps and specialized directories, each serving specific functionalities integral to the application's operation. It orchestrates the flow from command execution to data manipulation and persistence, leveraging a well-defined structure for efficient processing and management of web page data.

### Directory Structure

- `cli/`: Responsible for handling and executing custom Django management commands. This is the entry point for user-initiated commands.
  - `management/commands/`: Contains the definitions of custom Django management commands.

- `config/`: Houses the Django project configurations, including settings, URLs, and other global configurations.

- `core/`: The heart of the application, containing core entities like projects, websites, URLs, keywords, etc. This directory also handles data persistence.
  - `models/`: Definitions of Django models representing core entities.
  - `migrations/`: Database migration files for the `core` app's models.

- `exports/`: Manages data export functionalities, controlling and overseeing data gathering from various sources like manual CSV, Google Search Console (GSC), Google Analytics 4 (GA4), SEMrush exports, etc.

- `managers/`: Contains classes that manage and operate on the core entities, encapsulating the business logic for data manipulation.

- `workflows/`: Comprises classes that embody complex business logic. Workflows are triggered by `cli` commands and utilize managers to interact with `exports` and `core` components.

### Operational Flow

- **Command Line Interfaces (`cli`):** The entry point for user interactions. CLI commands can only execute workflows.
- **Workflows:** Contain and manage the execution logic. They orchestrate the sequence of operations and are solely responsible for invoking managers and dealing with exports. Workflows do not interact directly with core objects.
- **Managers (`managers`):** Handle all interactions with core objects (`core`). They encapsulate the business logic for manipulating core data but do not directly handle data exports.
- **Exports (`exports`):** Focused on data extraction and external data interactions. Managers use exports for data handling but exports do not interact directly with core objects.
- **Core Objects (`core`):** The fundamental entities of the application, such as projects, websites, and URLs. Managed exclusively by managers.

This structured separation ensures a clear and maintainable codebase, with each component focusing on its specific role.

### Usage
Instructions on how to use the application, including command-line interfaces:

Create a new project:
```
python manage.py create_project "Project Name" "https://example.com" "/path/to/data_folder"
```

Collect data for a project:
```
python manage.py collect_project_data_exports "Project Name"
```
