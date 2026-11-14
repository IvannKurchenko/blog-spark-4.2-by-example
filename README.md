# Spark 4.2 by example

## Introduction

This repository contains companion code for the blog post "Spark 4.2 by example".

## Prerequisites

The following tools must be installed before running the code:

- **Python 3.11**
- **Apache Spark 4.2**
- **uv** - Python package manager ([install guide](https://docs.astral.sh/uv/getting-started/installation/))

## How to

### Initial setup

Create a virtual environment and install dependencies:

```bash
make setup
make install
```

### Local stack

A Docker Compose stack provides a Unity Catalog server (with UI) and a remote
Spark 4.2 Connect server wired to it via the `unity` catalog. Delta Lake and the
Unity Catalog Spark connector are loaded as Spark packages.

```bash
make up    # start Unity Catalog + UI + Spark Connect server
make down  # stop the stack
```

Endpoints:

- Unity Catalog API: http://localhost:8080
- Unity Catalog UI: http://localhost:3000
- Spark Connect: `sc://localhost:15002`
- Spark UI: http://localhost:4040 (and http://localhost:8081)

Connect to the remote Spark session from Python:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost").getOrCreate()
spark.sql("SHOW CATALOGS").show()
```
