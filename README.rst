Record Recommender
==================

.. image:: https://travis-ci.org/CERNDocumentServer/record-recommender.svg?branch=master
    :target: https://travis-ci.org/CERNDocumentServer/record-recommender


The Record Recommender creates recommendations for Invenio.
By reading page views and downloads from Elasticsearch calculating the
recommendations and storing them in Redis to be retrieve and displayed
by Invenio.


Usage
-----

The workflow to generate the recommendations is:

1. ``recommender fetch 24`` to cache the last 24 weeks of page views and downloads
   from Elasticsearch.
2. ``recommender profiles 24`` generate the user profiles from the page views
   and downloads. For the not logged in users profiles based on the
   ip-address and user agent are created.
3. ``recommender build 50`` calculates the recommendations using 50 processes
   and stores them in the specified Redis server.

Alternative the recommendations can be automatically be fetched, the profiles
generated and the recommendations calculated all this with one command:

`recommender update_recommender 24 50` for the last 24 weeks and using
50 processes.



Configuration
-------------
The configuration file is expected to be in ``/etc/record_recommender.yml``
otherwise the path to the config file can be defined by using the
command line option ``--config_path``.

.. code-block:: yaml

    # Record-Recommender configuration.

    elasticsearch:
    es_index: ['index-2014', 'index-2015', 'index-2016']
    es_user: user
    es_password:
    es_host: localhost
    es_port: 443

    recommendation_version: 1

    # Sentry connection string
    sentry:

    redis:
    host: localhost
    port: 6379
    db: 0
    prefix: 'Reco_1::'

    cache:
    base_path: cache/
    cache_file_prefix: ''

    logging:
    version: 1
    disable_existing_loggers: False
    formatters:
        simple:
        format: '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    handlers:
        console:
        class: logging.StreamHandler
        level: INFO
        formatter: simple
        stream: ext://sys.stdout
        sentry:
        class: raven.handlers.logging.SentryHandler
        level: WARN
        dsn:
        file:
        class : logging.FileHandler
        formatter: simple
        level: DEBUG
        filename: log_recommender.log
    loggers:
        record_recommender:
        level: DEBUG
        handlers: [console, file, sentry]
        propagate: no
        # elasticsearch:
        #   level: WARN
        #   handlers: [console]
        #   propagate: no
    root:
        level: ERROR
        handlers: [console, file, sentry]

Additional to the configuration options found in the config file
environment variables, which overwrite the ones from the config file can be set.

- ``RECOMMENDER_ES_PASSWORD`` to set the Elasticsearch password.
- ``RECOMMENDER_SENTRY`` to set the Sentry connection string.




Command line
------------

.. code-block:: console

    Usage: recommender [OPTIONS] COMMAND [ARGS]...

    Record-Recommender command line version.

    Options:
    -v, --verbose           Enables verbose mode.
    -c, --config_path TEXT  Path to the configuration file.
    --help                  Show this message and exit.

    Commands:
    debug               Debug the application and recommender.
    fetch               Fetch newest PageViews and Downloads.
    build               Calculate all recommendations.
    profiles            Number of weeks to build.
    update_recommender  Download and build the recommendations.


Debugging the Recommendations
-----------------------------
As first step look into the created user profiles in the defined ``cache``
folder.

To explore the graph with all loaded data use the
``recommender debug`` command to get a interactive python shell.


