# Multinational Retail Data Centralisation

In this project I am tasked with extracting data from a multitude of sources and cleaning it before uploading it to a local postgres database. It demonstrates my use of python and various packages and modules to create utility classes for handling, cleaning, and extracting the data. These classes are designed to simplify common data handling tasks in a structured and modular way.

I learned a great deal working on this project, how to extract data from SQL tables, PDF files, API endpoints and S3 buckets. How to use VSCode to effectively clean and transform messy datasets into pandas dataframes...

### Scenario
You work for a multinational company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location. Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. You will then query the database to get up-to-date metrics for the business.

## Milestone 1 - Set up the environment
First I setup Github to track changes to my code and save them in a Github repo. I also setup an environment for the project.

#### Dependencies

    pandas: Data manipulation library for Python
    yaml: YAML parsing library for Python
    sqlalchemy: SQL toolkit and Object-Relational Mapping (ORM) for Python
    requests: HTTP library for Python
    boto3: Amazon Web Services (AWS) SDK for Python
    tabula: PDF table extraction library for Python

Install dependencies using:

```
    pip install pandas yaml sqlalchemy requests boto3 tabula
```

## Milestone 2 - Extract and clean the data from the data sources
First I set up a new database within pgadmin4 and named it `sales_data` which will store all the company information once processed.

Next I created the classes:

#### DatabaseConnector

The `DatabaseConnector` class facilitates the connection to a PostgreSQL database. It reads database credentials from a YAML file, initializes the SQLAlchemy engine, and provides methods for listing database tables and uploading data to the database.
#### DataExtractor

The `DataExtractor` class is designed to extract data from various sources. It can read data from a PostgreSQL table, retrieve data from a PDF file, list the number of stores from a given endpoint, and extract data from an S3 bucket.
#### DataCleaning

The `DataCleaning` class offers a suite of static methods for cleaning and transforming data in a pandas DataFrame. From removing null values to converting product weights, these methods cover a range of common data cleaning tasks.

The first set of data is stored in the cloud in an AWS database and contains the historical data of users. The credentials for which I've stored on a yaml file and can be accessed through the following function:

