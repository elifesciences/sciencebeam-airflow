# ScienceBeam Airflow

[Airflow](https://airflow.apache.org/) pipeline for ScienceBeam related training and evaluation.

[Apache Airflow](https://airflow.apache.org/):
> is a platform to programmatically author, schedule, and monitor workflows.
> ...
> Airflow is not a data streaming solution.

We are using the [Puckel Airflow Image](https://github.com/puckel/docker-airflow).

## Prerequisites

* [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)
* [Google Gloud SDK](https://cloud.google.com/sdk/docs/) for [gcloud](https://cloud.google.com/sdk/gcloud/)

## gcloud setup

`gcloud auth login`

## Configuration

Airflow, using the [Puckel Airflow Image](https://github.com/puckel/docker-airflow), is mainly configured in the following way:

* Environment variables interpreted by the [entrypoint](https://github.com/puckel/docker-airflow/blob/master/script/entrypoint.sh), e.g. `POSTGRES_HOST`
* Environment variables interpreted by [Airflow](http://airflow.apache.org/howto/set-config.html), e.g. `AIRFLOW__CORE__SQL_ALCHEMY_CONN`
* Default configuration in the [airflow.cfg](https://github.com/puckel/docker-airflow/blob/master/config/airflow.cfg) file
* Default configuration by the Airflow project in [default_airflow.cfg](https://github.com/apache/airflow/blob/master/airflow/config_templates/default_airflow.cfg)

(Since we are using Docker Compose, environment variables would be passed in via [docker-compose.yml](docker-compose.yml))

## Deployment

The [Dockerfile](Dockerfile) is used to build the image that is getting deployed within the cluster.

## Development

The Docker Compose configuration is only used for development purpose (in the future it could in part be used to build the image).

For development, it is making the local gcloud config available to the Airflow container.

### Start

Build and start the image.

```bash
make start
```

Airflow Admin will be available on [port 8080](http://localhost:8080/admin) and the Celery Flower will be on [port 5555](http://localhost:5555/).

### Test

Build and run tests.

```bash
make test
```

### Stop

```bash
make stop
```

### Clean

```bash
make clean
```
