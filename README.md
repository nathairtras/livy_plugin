# Plugin - Livy

This plugin submits jobs to Spark through REST requests to the [Livy API](https://livy.incubator.apache.org/examples/).

## Operators
### LivyOperator
This operator creates a session through Livy, submits a job, polls for completion, and terminates the session.


## Source
This repo was originally forked from [rssanders3/airflow-spark-operator-plugin](https://github.com/rssanders3/airflow-spark-operator-plugin).
The `BashOperator`-derived local submission operator has been removed to focus on enhancing Livy support.