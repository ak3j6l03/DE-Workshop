My Bruin project is located at https://github.com/ak3j6l03/Bruin-workshop.git.

## Question 1
A Bruin project (defined by `.bruin.yml`) can contain multiple independent pipelines, each with its own `pipeline.yml` and assets.

## Question 2
Use time_interval when you want to reprocess a specific date/time window by deleting and reinserting data for that period.

## Question 3
`bruin run --var 'taxi_types=["yellow"]'`

## Question 4
Use `--downstream` to run a modified asset together with all assets that depend on it.

## Question 5
Use the `not_null` quality check to ensure a column never contains NULL values.

## Question 6
Use `bruin lineage` to visualize the dependency graph (DAG) between assets in your pipeline.

## Question 7
Use `--full-refresh` on the first run to create all tables from scratch and populate them fully, bypassing incremental logic.