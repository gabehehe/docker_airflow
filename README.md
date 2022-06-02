# docker-airflow -editado

Créditos: puckel

## Installation

Para baixar a imagem original:

    docker pull puckel/docker-airflow

## Build

Neste projeto, para buildar, rodar o seguinte comando:

    docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t puckel/docker-airflow .

Não esquecer de atualizar as imagens do airflow no docker-compose (puckel/docker-airflow:latest).

## Usage

A imagem está rodando em modo Celery, simulando uma arquitetura em nuvem.

Para subir o arquivo docker-composer com n workers:

docker-compose up -d --scale worker=2
