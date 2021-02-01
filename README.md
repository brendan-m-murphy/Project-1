# Data Modeling with PostgreSQL

The analytics team at a fictitious music streaming company, Sparkify, want to understand what their users are listening to.
The goal of this project is to take .json data on Sparkify's music library and user activity and load it into a PostgreSQL database in a format that is useful for analytics.

## Description
Our end goal is to load a collection of .json files into a Postgres database with a star schema, which we've chosen for analytics processing.

The module `staging.py` defines a `Stager` class, which loads all of the .json files in a directory into a temporary staging table.
The files are transformed according to a function passed the instance of `Stager`.
The `Stager` class has a copy method, which uses the Postgres `COPY` function to load the files into the database.

An auxiliary class `StringIteratorIO` (in a separate module) creates a file-like generator to feed the `COPY` function; this allows the files to be processed and copied without holding all of the transformed files in memory.

The file `etl.py` defines the transformer functions for transforming song and event data, which are used to define `Stager`s for each type of data.
In `main()`, the data is staged, then inserted into the star schema using the queries in `sql_queries.py`.


## Usage

- Change the dsn in the `create_database` function in `create_tables.py` to a user with permission to create and drop tables.
- Run `etl.py`, which invokes `create_tables.py` and prints the result of a simple query that checks if `songplays` has an entry with non-null `artist_id`.