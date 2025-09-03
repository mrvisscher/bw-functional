import tqdm
from logging import getLogger

import bw2data as bd
from bw2data.backends import SQLiteBackend

from .database import FunctionalSQLiteDatabase

log = getLogger(__name__)


def convert_sqlite_to_functional_sqlite(database_dict: dict) -> dict:
    return SQLiteToFunctionalSQLite.convert(database_dict)


class SQLiteToFunctionalSQLite:
    @classmethod
    def convert(cls, database_dict: dict):
        converted = {}

        for key, ds in tqdm.tqdm(database_dict.items()):
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


def convert_functional_sqlite_to_sqlite(database_dict: dict):
    return FunctionalSQLiteToSQLite.convert(database_dict)


class FunctionalSQLiteToSQLite:
    @classmethod
    def convert(cls, database_dict: dict):
        converted = {}

        for key, ds in tqdm.tqdm(database_dict.items()):
            if ds["type"] in ["product", "waste"]:
                processor = database_dict[ds["processor"]]
                converted[key] = cls.convert_function(key, ds, processor)

        return converted

    @classmethod
    def convert_function(cls, key, ds, processor):
        ds["type"] = "processwithreferenceproduct"
        ds["product"] = ds["name"]
        ds["name"] = processor["name"]

        ds["exchanges"] = []
        del ds["processor"]

        for exc in processor["exchanges"]:
            exc = exc.copy()
            exc["output"] = key

            if exc["type"] == "production" and exc["input"] != key:
                # production exchange of another function than this one
                continue

            if exc["type"] != "production" and ds.get("allocation_factor"):
                log.info(f"Allocating exchange from {exc['input']} to {ds['name']} "
                         f"with factor {ds['allocation_factor']}")
                exc["amount"] = exc["amount"] * ds['allocation_factor']
                if exc.get("formula"):
                    log.info(f"Allocating formula from {exc['input']} to {ds['name']}: "
                             f"{exc['formula']} * {ds['allocation_factor']}")
                    exc["formula"] = f"{exc['formula']} * {ds['allocation_factor']}"
                if exc.get("uncertainty type"):
                    log.warning(f"Exchange from {exc['input']} to {ds['name']} has an uncertainty distribution that "
                                f"will not be allocated")

            ds["exchanges"].append(exc)

        return ds
