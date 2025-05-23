# Playground

![Playground UI](./images/playground.png)

This repository contains the example queries used across all our documentation and blogs, it also contains the script to set up the users. 

Users can raise UI issues in this repository and suggest example queries.

## What is the Playground?

The [Playground](https://sql.clickhouse.com) powers the example you see when reading the ClickHouse [documentation](https://clickhouse.com/docs) or [blogs](https://clickhouse.com/blog). 

Each time you see in the blogs a widget showing a SQL query with the ![Pen icon](./images/pen.png) icon, you can click on it to open the Playground where you can run the query or build a simple visualization.

![Widget](./images/widget.png)

## Datasets

The playground has a number of datasets which we try to keep up-to-date, including but not limited to:

- github - Contains GitHub activity data, repositories, and user interactions. Updated hourly.
- pypi - a row for every Python package downloaded with pip, updated daily - over 1.3 trillion rows
- rubygems - a row for every gem installed - updated hourly - over 180 billion rows
- hackernews - Contains posts and comments from Hacker News
- imdb- Contains movie database information from IMDB
- nyc_taxi - Contains NYC taxi trip data
- opensky - Contains aviation data from the OpenSky Network
- reddit - Contains posts and comments from Reddit
- stackoverflow - Contains questions and answers from Stack Overflow
- uk - contains a comprehensive collection of UK property transaction data and related geographical information

## Contributing example queries

Fork this repository and raise a PR on the [queries.json](./queries.json) file. Once the PR is approved, this query will be deployed to the ClickHouse instance (currently manually).

Make sure to not modify the id of existing queries.

## Reporting UI or Performance issues with Playground

Raise an issue in this repository using the relevant template.

## Direct connections

Users can connect directly to the ClickHouse instance using the [ClickHouse Client](https://clickhouse.com/docs/en/interfaces/cli).

```bash
clickhouse client --host sql-clickhouse.clickhouse.com --secure --user demo --password ''
```

## Quotas

Users connecting to the playground are subject to quotas to ensure fair usage and a stable service. Specifically:

- 200 queries per hour
- Total of 6000s execution time per hour
- Total of 3b result rows per hour
- Max 60s query execution time
- Max 10b rows read per query
- Max 30GB of memory per query
- Max 1TB read per query

On hitting a limit the current results will be returned - result sets may therefore be incomplete.

## Load scripts [WIP]

This [folder](./load_scripts) contains the scripts use to keep the data in the playground up to date. We rely on Google Cloud Run to execute the scripts. 

Each folder contains a `Dockerfile`, a bash script to ingest the data and a `cloudbuild.yaml` that describe how to deploy to Cloud Run job. Each job relies on environment variables to run that are listed in the individual folder.

If you interested in reproducing in your own Cloud Run instance, you can use the following command to deploy a new version. 

```bash
cd load_scripts/<dataset-name>
# Make sure you are logged in with gcloud and the env variable PROJECT_ID is set
gcloud builds submit --config cloudbuild.yaml .
```

This can also be used locally.


