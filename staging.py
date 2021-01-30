import io
import os
import glob
import psycopg2
from psycopg2 import sql
import pandas as pd
import sql_queries


def extract(filepath):
    """
    Yields all .json files from filepath.
    """
    for root, _, _ in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            yield f


class StringIteratorIO(io.TextIOBase):
    """
    File-like object.
    Needs to implement read() and readline().
    """
    pass


class Stager:
    """
    A class that writes .json files to a temporary PostgreSQL table.

    The most important attribute of a Stager is its transformer, which
    must be carefully designed to match the input files to the staging table.
    
    The rest of the class encapsulates creating, copying, and dropping
    the staging table.

    WARNING: this class is not safe to use with untrusted parameters,
    due to use of string formatting to create and drop tables.

    It is also possible to over-write a table using the create_table() method.
    Make sure that table_name does not already exist.

    Attributes
    ----------

    filepath: string or path-like object

    table_name: string, name of staging table

    columns: dict or list, names of columns in staging table, with data types
    as values, if passed a dictionary.

    transformer: function, takes a filepath to a .json file and returns
    a CSV string with \t separator and null values as a null string.

    """
    def __init__(self, filepath, table_name, columns, transformer):
        self.filepath = filepath
        self.table_name = table_name
        if isinstance(columns, dict):
            self.columns = columns
        elif isinstance(columns, list):
            self.columns = {k: 'TEXT' for k in columns}
        else:
            raise TypeError('columns must be a dict or list')
        self.transformer = transformer

    def get_table_name(self):
        return self.table_name

    def set_columns(self, cols):
        self.columns = cols

    def get_columns(self, dtypes=False):
        """
        Returns column names as a list.

        If dtypes=True, returns column names and
        their data types as a dict.
        """
        if dtypes:
            return self.columns
        else:
            return self.columns.keys()

    def copy(self, cur):
        with io.StringIO() as f:
            for file in extract(self.filepath):
                f.write(self.transformer(file))
            f.seek(0)
            cur.copy_from(f, self.table_name, columns=tuple(self.get_columns()), null='')

    def create_table(self, cur):
        cols_str = ', '.join([k + ' ' + v for k, v in self.columns.items()])
        query = 'CREATE UNLOGGED TABLE {} (id SERIAL, {});'.format(self.table_name, cols_str)
        cur.execute(query)

    def drop_table(self, cur):
        query = 'DROP TABLE {};'.format(self.table_name)
        cur.execute(query)
