# S3 to Data Lake Pipeline

Loads S3 json files into the spark engine, transform them, and output them as partioned parquet analytics file in S3.

# Motivation

Experimentation and learning of S3, Spark and Data Lakes. This project was created as part of the Data Engineering Nano Degree, run by Udacity.

# The Project Scenario

*A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.*

*As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.*

# Data Sets 

The pipeline processes 2 types of JSON data file

## Song dataset
Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID

## Log dataset
The log datasets contain activity logs from the music streaming app, and are partitioned by year and month.

# Screenshots

![Redshift](media/pyspark.png)
# Tech used

Spark
Amazon S3
Parquet

**Built with**
- The PySpark API for Spark (Python)
- Python, SQL and PySpark for ETL pipeline

# Features

- Utilizes Spark for scalable processing
- Simulates a common data lakes analytical flow
- Outputs to parquet files to allow for fast analytics processing

# Running the process

The process is typically run in the following order:

- [etl.py](etl.py) - to load, process and output data

# More detail on script files and their purpose

[etl.py](etl.py)
- loads files from input storage
- transforms data using Spark
- outputs to an analytics folder in S3