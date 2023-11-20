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
  - `managers/`: Contains classes that manage and operate on the core entities, encapsulating the business logic for data manipulation.

- `exports/`: Manages data export functionalities, controlling and overseeing data gathering from various sources like manual CSV, Google Search Console (GSC), Google Analytics 4 (GA4), SEMrush exports, etc.

- `workflows/`: Comprises classes that embody complex business logic. Workflows are triggered by `cli` commands and utilize managers to interact with `exports` and `core` components.

### Operational Flow

- **Command Line Interfaces (`cli`):** The entry point for user interactions. CLI commands can only execute workflows.
- **Workflows:** Contain and manage the execution logic. They orchestrate the sequence of operations and are solely responsible for invoking managers and dealing with exports. Workflows do not interact directly with core objects.
- **Managers (`managers`):** Handle all interactions with core objects (`core`). They encapsulate the business logic for manipulating core data but do not directly handle data exports.
- **Core Objects (`core`):** The fundamental entities of the application, such as projects, websites, and URLs. Managed exclusively by managers.
- **Exports (`exports`):** Focused on data extraction and external data interactions. Managers use exports for data handling but exports do not interact directly with core objects.

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
## Logging Guidelines

In this project, we maintain specific standards for implementing logging to ensure clarity and consistency. Please adhere to the following guidelines when contributing to the codebase:

- **Targeted Logging**: Logging should be implemented selectively within certain modules of the project. The modules where logging is expected are:
  - `Manager`: Any management-related functionalities should include detailed logging.
  - `Workflow`: Logging is crucial in workflow components to track the process flow and identify any issues.
  - `Export`: Implement logging in export functionalities to monitor data exporting processes.
  - `Report`: Reporting modules should include logging to track report generation and any associated computations.

- **Avoid Logging in Certain Modules**: To maintain code cleanliness and prevent log clutter, please refrain from adding logging in the following areas:
  - `CLI (Command Line Interface)`: The CLI should remain clean and user-friendly without logging details.
  - `Core`: The core functionalities of the application should focus on performance and algorithmic efficiency without the overhead of logging.

- **Logging Standards**: When implementing logging, please ensure that:
  - The log messages are clear, concise, and meaningful.
  - Log levels (like INFO, DEBUG, ERROR) are appropriately used to differentiate the importance and type of logged information.
  - Sensitive information (like user data or credentials) should never be logged.

- **Code Review and Compliance**: All contributions will be reviewed for compliance with these logging guidelines. Please ensure your code adheres to these standards before submitting a pull request.

Following these guidelines will help maintain a clean, efficient, and maintainable codebase. If you have any questions or need clarification on logging practices, feel free to reach out to the project maintainers.
