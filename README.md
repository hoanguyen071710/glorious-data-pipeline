# Data Engineering Project: Data pipeline for stock database

## Introduction
This project is a collaborative effort involving three members, aimed at building an end-to-end data engineering pipeline for a fake generated stock platform business. The primary objective of this project is to simulate the entire data flow from data generation to data warehousing, enabling us to gain hands-on experience in data engineering concepts and technologies.

## Components:
The project includes the following key components:
- **RDS (Database):** We use MySQL hosted on AWS RDS to store the generated data.
- **AWS EC2 (VM):** Amazon Elastic Compute Cloud (EC2) is utilized to run the necessary services and applications.
- **PostgreSQL (Data Warehouse):** We use PostgreSQL to build the data warehouse, allowing efficient querying and analysis.
- **AWS S3 (Data Lake):** Amazon Simple Storage Service (S3) serves as the data lake for storing raw and processed data.
- **Cron (ETL):** Cron jobs are set up to schedule and automate the ETL (Extract, Transform, Load) processes.
- **SQLAlchemy (ETL):** We leverage SQLAlchemy to interact with the database and perform ETL operations.
- **Pandas (ETL):** The Pandas library is used for data manipulation and transformation during ETL.
- **Boto3 (ETL):** Boto3 is used to interact with AWS S3 for data upload and download during the ETL process.
- **Git (Teamwork):** Git version control system is employed for collaborative development and code management.

![Data Engineering Project Components](path/to/your/image.png)

## Situation
In this project, we randomly generate data into the RDS database at specific intervals, mimicking a real stock platform database. We use Pandas, SQLAlchemy, and Cron to schedule and snapshot data daily, implementing both the Hard Delete Pattern and Soft Delete Pattern to minimize loads on the production database.

The database we use is MYSQL hosted on AWS RDS. We employ Pandas, SQLAlchemy, and Cron to manage the data updates and perform daily snapshots using both the Hard Delete Pattern and Soft Delete Pattern.

We then detect changes with these patterns (Delete and Soft) to load data into the Data Warehouse. The fact table in the Data Warehouse is designed as a transaction fact table, while the user and stock tables follow Slowly Changing Dimension (SCD) techniques for historical query purposes.

For example, we query whether the customers would buy more stocks after their email address got changed.

![Data Model Image](path/to/your/datamodelimage.png)

## Design
In our data model design, we have the following considerations:
- The user table is designed to enable the Soft Delete Pattern for handling data deletions.
- The stock table does not have a change indicator column, so we must use the Hard Delete Pattern to handle updates.

![Entity Relationship Diagram (ERD) Image](path/to/your/erdimage.png)

## Tasks
The project encompasses the following major tasks:
- Generate fake data using the Faker library and insert it into the RDS database.
- Set up cron jobs to schedule and automate data generation and ETL processes.
- Utilize SQLAlchemy and Boto3 for efficient data upload and download between different components.
- Handle datetime conversions, considering both UTC and Asia/Ho_Chi_Minh (HCM) timezones.

## Result
Upon completing the data pipeline, we will have dimensions and fact data that can be joined together for analysis and reporting.

## Improvements
Moving forward, we plan to make the following improvements to the project:
- Add DATE dimension to enhance time-based querying capabilities.
- Implement automated tests to ensure data integrity and reliability.
- Refactor the codebase to improve code readability, maintainability, and performance.

Feel free to contribute to the project by suggesting additional improvements or adding new features! Let's collaborate and enhance our data engineering skills together!