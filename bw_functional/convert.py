import tqdm

import bw2data as bd
from bw2data.backends import SQLiteBackend


def convert_sqlite_to_functional_sqlite(database_name: str) -> dict:
    return SQLiteToFunctionalSQLite.convert(database_name)


class SQLiteToFunctionalSQLite:
    @classmethod
    def convert(cls, database_name: str):
        db = bd.Database(database_name)

        if not isinstance(db, SQLiteBackend):
            raise TypeError("Database is not of type SQLite.")

        converted = {}

        for key, ds in tqdm.tqdm(db.load().items()):
            if ds["type"] in ["process", "processwithreferenceproduct"]:
                converted.update(cls.convert_process(key, ds))

        return converted

    @classmethod
    def convert_process(cls, key, ds):
        ds["type"] = "process"
        production = [x for x in enumerate(ds["exchanges"]) if x[1]["type"] == "production"]
        if len(production) > 1:
            # Check if the process has multiple production exchanges
            # and raise an error if so
            act = bd.get_activity(key)
            raise ValueError("Cannot convert a process with multiple production exchanges to functional_sqlite.", act)

        cls.convert_exchanges(key, ds)

        if not production:
            function_key, function = cls.create_function(key, ds)
        else:
            index, exchange = production.pop()
            function_key, function = cls.create_function(key, ds, amount=exchange["amount"])
            ds["exchanges"].pop(index)

        return {key: ds, function_key: function}

    @staticmethod
    def create_function(key, ds, amount=1.0, name=None):
        function_name = name or ds.get("reference product") or ds.get("product") or ds.get("name")
        function_code = ds["code"] + "_function"
        function_key = (key[0], function_code)

        function = {
            "type": "product" if amount > 0 else "waste",
            "name": function_name,
            "exchanges": [],
            "database": ds["database"],
            "code": function_code,
            "processor": key,
            "location": ds.get("location"),
            "unit": ds.get("unit"),
        }

        ds["exchanges"].append({
            "type": "production",
            "input": function_key,
            "output": key,
            "amount": amount,
        })

        return function_key, function

    @staticmethod
    def convert_exchanges(key, ds) -> None:
        excs = ds["exchanges"]
        database, code = key

        for exc in excs:
            if exc["input"][0] != database:
                continue
            exc["input"] = (database, exc["input"][1] + "_function")

def convert_functional_sqlite_to_sqlite(database_name: str) -> None:
    """
    Convert a database to the functional_sqlite backend.

    This function converts a database to the functional_sqlite backend by copying the data
    from the original database and updating the backend type.

    Args:
        database_name (str): The name of the database to convert.

    Raises:
        ValueError: If the database is already of type functional_sqlite.
    """
    from bw2data import Database

    db = Database(database_name)
    if db.backend_type == "functional_sqlite":
        raise ValueError("Database is already of type functional_sqlite.")

    db.copy(database_name, backend="functional_sqlite")

