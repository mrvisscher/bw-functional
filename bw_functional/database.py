import sqlite3
import pickle
import datetime
from logging import getLogger
from time import time
from typing import Any

import pandas as pd
from fsspec.implementations.zip import ZipFileSystem

from bw_processing import clean_datapackage_name, create_datapackage
from bw2data.backends import SQLiteBackend, sqlite3_lci_db
from bw2data.backends.schema import ActivityDataset
from pandas._libs.missing import NAType

from .node_dispatch import functional_node_dispatcher

log = getLogger(__name__)


def functional_dispatcher_method(
    db: "FunctionalSQLiteDatabase", document: ActivityDataset = None
):
    return functional_node_dispatcher(document)


class FunctionalSQLiteDatabase(SQLiteBackend):
    """A database which includes multifunctional processes (i.e. processes which have more than one
    functional input and/or output edge). Such multifunctional processes normally break square
    matrix construction, so need to be resolved in some way.

    We support three options:

    * Mark the process as `"skip_allocation"=True`. You have manually constructed the database so
        that is produces a valid technosphere matrix, by e.g. having two multifunctional processes
        with the same two functional edge products.
    * Using substitution, so that a functional edge corresponds to the same functional edge in
        another process, e.g. combined heat and power produces two functional products, but the
        heat product is also produced by another process, so the amount of that other process would
        be decreased.
    * Using allocation, and splitting a multifunctional process in multiple read-only single output
        unit processes. The strategy used for allocation can be changed dynamically to investigate
        the impact of different allocation approaches.

    This class uses custom `Node` classes for multifunctional processes and read-only single-output
    unit processes.

    Stores default allocation strategies per database in the `Database` metadata dictionary:

    * `default_allocation`: str. Reference to function in `multifunctional.allocation_strategies`.

    Each database has one default allocation, but individual processes can also have specific
    default allocation strategies in `MultifunctionalProcess['default_allocation']`.

    Allocation strategies need to reference a process `property`. See the README.

    """

    backend = "functional_sqlite"
    node_class = functional_dispatcher_method

    def relabel_data(self, data: dict, old_name: str, new_name: str) -> dict:
        """
        Changing relabel data to also incorporate changing `processor` on database copy
        """
        def relabel_exchanges(obj: dict, old_name: str, new_name: str) -> dict:
            for e in obj.get("exchanges", []):
                if "input" in e and e["input"][0] == old_name:
                    e["input"] = (new_name, e["input"][1])
                if "output" in e and e["output"][0] == old_name:
                    e["output"] = (new_name, e["output"][1])

            if obj.get("processor") and obj.get("processor")[0] == old_name:
                obj["processor"] = (new_name, obj["processor"][1])

            return obj

        return dict(
            [((new_name, code), relabel_exchanges(act, old_name, new_name)) for (db, code), act in data.items()]
        )

    def register(self, **kwargs):
        if "default_allocation" not in kwargs:
            kwargs["default_allocation"] = "equal"
        super().register(**kwargs)

    def process(self, csv: bool = False, allocate: bool = True) -> None:
        tech_matrix, bio_matrix, dependents = self.build_matrices()

        self.metadata["processed"] = datetime.datetime.now().isoformat()

        fp = str(self.dirpath_processed() / self.filename_processed())

        dp = create_datapackage(
            fs=ZipFileSystem(fp, mode="w"),
            name=clean_datapackage_name(self.name),
            sum_intra_duplicates=True,
            sum_inter_duplicates=False,
        )
        self._add_inventory_geomapping_to_datapackage(dp)

        dp.add_persistent_vector_from_iterator(
            matrix="biosphere_matrix",
            name=clean_datapackage_name(self.name + " biosphere matrix"),
            dict_iterator=bio_matrix.to_dict('records'),
        )

        dp.add_persistent_vector_from_iterator(
            matrix="technosphere_matrix",
            name=clean_datapackage_name(self.name + " technosphere matrix"),
            dict_iterator=tech_matrix.to_dict('records'),
        )

        dp.finalize_serialization()

        self.metadata["depends"] = list(dependents)
        self.metadata["dirty"] = False
        self._metadata.flush()

    def build_matrices(self) -> (pd.DataFrame, pd.DataFrame, set):
        nodes, exchanges, dependents = self.get_tables()

        tech_matrix = self.technosphere(nodes, exchanges)
        tech_matrix = pd.concat([tech_matrix, self.production(nodes, exchanges)])
        tech_matrix = pd.concat([tech_matrix, self.substitution(nodes, exchanges)])

        bio_matrix = self.biosphere(nodes, exchanges)

        return tech_matrix, bio_matrix, dependents

    def technosphere(self, nodes, exchanges):
        # bind technosphere flows

        # join all processor exchanges to the function and allocate them based on the allocation_factor
        x = nodes.merge(exchanges.loc[exchanges["type"] == "technosphere"], left_on="processor", right_on="output")
        x["amount"] = x["allocation_factor"].fillna(0) * x["amount"]
        x["flip"] = True
        x.rename(columns={"id": "col", "input": "row"}, inplace=True)

        # bind substituted flows

        # join all the production amounts from the substitution (needed for nomalization)
        y = nodes.loc[nodes["substitution_factor"] > 0].merge(
            exchanges.loc[exchanges["type"] == "production"],
            left_on="substitute", right_on="input"
        ).rename(columns={"amount": "sub_amount"})

        # join all the production amounts from the function itself (needed for nomalization)
        y = y.merge(
            exchanges.loc[exchanges["type"] == "production"],
            left_on="id", right_on="input"
        ).rename(columns={"amount": "self_amount"})

        # normalize the production amounts and divide it by the substitution_factor
        y["amount"] = (y["self_amount"] / y["sub_amount"]) / y["substitution_factor"]
        y["flip"] = True
        y.rename(columns={"id": "col", "substitute": "row"}, inplace=True)

        return pd.concat([x[["row", "col", "amount", "flip"]], y[["row", "col", "amount", "flip"]]])

    def production(self, nodes, exchanges):
        x = nodes.merge(
            exchanges.loc[exchanges["type"] == "production"],
            left_on=["id", "processor"],
            right_on=["input", "output"]
        )
        x["flip"] = False
        x.rename(columns={"id": "col", "input": "row"}, inplace=True)
        return x[["row", "col", "amount", "flip"]]

    def substitution(self, nodes, exchanges):
        x = nodes.loc[nodes["substitution_factor"] > 0][["id", "processor"]].merge(
            nodes.loc[
                (nodes["substitution_factor"] <= 0) |
                (nodes["substitution_factor"].isna())]
            [["id", "processor", "allocation_factor"]],
            left_on="processor",
            right_on="processor"
        )

        x = x.merge(
            exchanges.loc[exchanges["type"] == "production"][["input", "amount"]],
            left_on="id_x",
            right_on="input"
        )

        x["amount"] = x["allocation_factor"].fillna(1) * x["amount"]

        x["flip"] = False
        x.rename(columns={"id_y": "col", "id_x": "row"}, inplace=True)
        return x[["row", "col", "amount", "flip"]]

    def biosphere(self, nodes, exchanges):
        x = nodes.merge(exchanges.loc[exchanges["type"] == "biosphere"], left_on="processor", right_on="output")
        x["amount"] = x["allocation_factor"].fillna(1) * x["amount"]
        x["flip"] = False
        x.rename(columns={"id": "col", "input": "row"}, inplace=True)
        return x[["row", "col", "amount", "flip"]]

    def get_tables(self) -> (pd.DataFrame, pd.DataFrame, set):
        t = time()
        con = sqlite3.connect(sqlite3_lci_db._filepath)

        def id_mapper(key) -> NAType | int:
            if not isinstance(key, tuple):
                return pd.NA
            try:
                return id_map_dict[key]
            except KeyError:
                raise KeyError(f"Node key {key} not found.")

        id_map = pd.read_sql(f"SELECT id, database, code FROM activitydataset", con)
        id_map["key"] = id_map.loc[:, ["database", "code"]].apply(tuple, axis=1)
        id_map_dict = id_map.set_index("key")["id"].to_dict()

        raw = pd.read_sql(f"SELECT data FROM activitydataset WHERE database = '{self.name}'", con)
        node_df = pd.DataFrame([pickle.loads(x) for x in raw["data"]],
                               columns=["database", "code", "type", "processor", "allocation_factor", "substitute", "substitution_factor"])
        node_df = node_df.merge(id_map[["database", "code", "id"]], on=["database", "code"])
        node_df["processor"] = node_df["processor"].map(id_mapper).astype("Int64")
        node_df["substitute"] = node_df["substitute"].map(id_mapper).astype("Int64")
        node_df = node_df[["id", "type", "processor", "allocation_factor", "substitute", "substitution_factor"]]

        raw = pd.read_sql(f"SELECT data, input_database FROM exchangedataset WHERE output_database = '{self.name}'", con)
        exc_df = pd.DataFrame([pickle.loads(x) for x in raw["data"]], columns=["input", "output", "type", "amount"])
        exc_df["input"] = exc_df["input"].map(id_mapper).astype("Int64")
        exc_df["output"] = exc_df["output"].map(id_mapper).astype("Int64")

        dependents = set(raw["input_database"].unique())
        if self.name in dependents:
            dependents.remove(self.name)

        con.close()

        log.debug(f"Processing: built tables from SQL in {time() - t:.2f} seconds")

        return node_df, exc_df, dependents
