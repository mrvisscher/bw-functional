import sqlite3
import pickle
import datetime
from logging import getLogger
from time import time
from typing import Any

import numpy as np
import pandas as pd
from fsspec.implementations.zip import ZipFileSystem

import stats_arrays as sa

from bw_processing import clean_datapackage_name, create_datapackage
from bw2data.backends import SQLiteBackend, sqlite3_lci_db
from bw2data.backends.schema import ActivityDataset
from pandas._libs.missing import NAType

from .node_dispatch import functional_node_dispatcher

log = getLogger(__name__)

UNCERTAINTY_FIELDS = ["uncertainty_type", "loc", "scale", "shape", "minimum", "maximum"]

def functional_dispatcher_method(
        db: "FunctionalSQLiteDatabase", document: ActivityDataset = None
):
    return functional_node_dispatcher(document)


class FunctionalSQLiteDatabase(SQLiteBackend):
    """

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
        nodes, exchanges, dependents = self.get_tables()
        exchanges = Mutate.set_default_uncertainty_values(exchanges)

        tech_matrix = Build.technosphere(nodes, exchanges)
        bio_matrix = Build.biosphere(nodes, exchanges)

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
                               columns=["database", "code", "type", "processor", "allocation_factor", "substitute",
                                        "substitution_factor"])
        node_df = node_df.merge(id_map[["database", "code", "id"]], on=["database", "code"])
        node_df["processor"] = node_df["processor"].map(id_mapper).astype("Int64")
        node_df["substitute"] = node_df["substitute"].map(id_mapper).astype("Int64")
        node_df = node_df[["id", "type", "processor", "allocation_factor", "substitute", "substitution_factor"]]

        raw = pd.read_sql(f"SELECT data, input_database FROM exchangedataset WHERE output_database = '{self.name}'",
                          con)
        exc_df = pd.DataFrame([pickle.loads(x) for x in raw["data"]],
                              columns=["input", "output", "type", "amount", "uncertainty type"] + UNCERTAINTY_FIELDS)

        exc_df.update(exc_df["uncertainty_type"].rename("uncertainty type"))
        exc_df["uncertainty_type"] = exc_df["uncertainty type"]
        exc_df.drop(["uncertainty type"], axis=1, inplace=True)

        exc_df["input"] = exc_df["input"].map(id_mapper).astype("Int64")
        exc_df["output"] = exc_df["output"].map(id_mapper).astype("Int64")

        dependents = set(raw["input_database"].unique())
        if self.name in dependents:
            dependents.remove(self.name)

        con.close()

        log.debug(f"Processing: built tables from SQL in {time() - t:.2f} seconds")

        return node_df, exc_df, dependents


class Build:
    @staticmethod
    def technosphere(nodes, exchanges):
        consumption = Build.consumption(nodes, exchanges)
        production = Build.production(nodes, exchanges)
        return pd.concat([consumption, production])

    @staticmethod
    def biosphere(nodes, exchanges):
        x = Build.allocated(nodes, exchanges, ["biosphere"])
        x["flip"] = False
        return x[["row", "col", "amount", "flip"] + UNCERTAINTY_FIELDS]

    @staticmethod
    def consumption(nodes, exchanges):
        x = Build.allocated(nodes, exchanges, ["technosphere"])
        x["flip"] = True
        return x[["row", "col", "amount", "flip"] + UNCERTAINTY_FIELDS]

    @staticmethod
    def production(nodes, exchanges):
        x = Join.production_exchanges_to_functions(nodes, exchanges)

        x["flip"] = False
        x.rename(columns={"input": "row", "output": "col"}, inplace=True)

        return x[["row", "col", "amount", "flip"] + UNCERTAINTY_FIELDS]

    @staticmethod
    def allocated(nodes, exchanges, exchange_types):
        x = Join.exchanges_to_functions(nodes, exchanges, exchange_types)
        x = Mutate.allocate_amount(x)
        x = Mutate.allocate_distributions(x)
        x.rename(columns={"input": "row", "output": "col"}, inplace=True)

        return x[["row", "col", "amount"] + UNCERTAINTY_FIELDS]


class Mutate:
    @staticmethod
    def allocate_amount(df: pd.DataFrame) -> pd.DataFrame:
        """
        `bw_functional` allocates at processing time by multiplying the amount for non-functional exchanges by the
        allocation factor of the function.

        This function takes a dataframe with an amount and allocation_factor column and returns a dataframe with
        allocated amounts.
        """
        df["amount"] = df["allocation_factor"].fillna(1) * df["amount"]
        return df

    @staticmethod
    def allocate_distributions(df: pd.DataFrame) -> pd.DataFrame:
        """
        To make stochastic modelling work for allocated process-functions we need to allocate the uncertainty
        distributions as well. We can use a standard method for this for most distributions, where we multiply the LOC,
        SCALE, MINIMUM and MAXIMUM with the allocation factor. The LogNormalUncertainty requires a different approach
        where we add the LN of the allocation factor to the LOC.

        Currently unsupported distributions for this are: Bernoulli, Discrete Uniform, Beta, Student's T. Though this
        may be fixed through the `stats_arrays` package in the future.

        This function takes a DataFrame with uncertainty columns and an allocation_factor and applies said
        allocation_factor to the distributions. Warning the user if any unsupported distributions have allocation
        factors.
        """
        # distributions that use the standard method
        standard = [sa.NormalUncertainty.id, sa.UniformUncertainty.id, sa.TriangularUncertainty.id,
                    sa.WeibullUncertainty.id, sa.GammaUncertainty.id, sa.GeneralizedExtremeValueUncertainty.id,
                    sa.UndefinedUncertainty, sa.NoUncertainty]
        # lognormal uncertainty
        ln = [sa.LognormalUncertainty.id]

        labels = (
                # if the uncertainty is not in either [standard] or [ln] AND
                (~df["uncertainty_type"].isin(standard + ln)) &
                # the allocation factor is not undefined OR equal to one
                (df["allocation_factor"].fillna(1) != 1).any(axis=None)
        )
        # warn the user
        if pd.Series.any(labels):
            log.warning("Database contains distributions that cannot be allocated")

        # apply the standard method to applicable distributions
        labels = df["uncertainty_type"].isin(standard)
        df.loc[labels, "loc"] = df.loc[labels, "loc"] * df.loc[labels, "allocation_factor"].fillna(1)
        df.loc[labels, "scale"] = df.loc[labels, "scale"] * df.loc[labels, "allocation_factor"].fillna(1)
        df.loc[labels, "minimum"] = df.loc[labels, "minimum"] * df.loc[labels, "allocation_factor"].fillna(1)
        df.loc[labels, "maximum"] = df.loc[labels, "maximum"] * df.loc[labels, "allocation_factor"].fillna(1)

        # apply the LN method to lognormal distributions
        labels = df["uncertainty_type"].isin(ln)
        df.loc[labels, "loc"] = df.loc[labels, "loc"] + np.log(df.loc[labels, "allocation_factor"].fillna(1))

        return df

    @staticmethod
    def set_default_uncertainty_values(df: pd.DataFrame) -> pd.DataFrame:
        """
        Datapackages expect a valid stats array for each exchange. This means that for exchanges that have no
        uncertainty associated, 'UndefinedUncertainty' must be set, with the exchanges' amount value as LOC.

        This function sets all non-defined uncertainties to UndefinedUncertainty. And sets the loc value for all
        UndefinedUncertainty and NoUncertainty entries that have no LOC predefined to the amount value of the exchange.
        """
        # set all undefined uncertainties to the UndefinedUncertainty type
        df["uncertainty_type"] = df["uncertainty_type"].fillna(sa.UndefinedUncertainty.id)

        labels = (
                # if the uncertainty type is either UndefinedUncertainty or NoUncertainty AND
                df["uncertainty_type"].isin([sa.UndefinedUncertainty.id, sa.NoUncertainty.id]) &
                # if LOC is not defined
                (df["loc"].isna())
        )
        # replace LOC with the exchange amount
        df.loc[labels, "loc"] = df.loc[labels, "amount"]
        return df


class Join:
    """
    `bw_functional` creates a square matrix by only using the function ids as row and column indexes. All exchanges
    of a process must therefore be bound to it's functions instead. So we join the nodes and exchanges dataframe
    based on the 'processor' of the node and the 'output' of the exchange. This class contains utility functions to do
    just that.
    """

    @staticmethod
    def exchanges_to_functions(
            nodes: pd.DataFrame,
            exchanges: pd.DataFrame,
            exchange_types: tuple | list,
            keep=("allocation_factor",)
    ) -> pd.DataFrame:
        """
        Joins exchanges of type `exchange_type` from the processes to all the functions of the processes.
        """
        exchanges = exchanges.loc[exchanges["type"].isin(exchange_types)]
        functions = nodes.dropna(subset="processor").drop("type", axis=1)

        joined = functions.merge(exchanges, left_on="processor", right_on="output")
        joined = joined.drop(["processor", "output"], axis=1)
        joined = joined.rename(columns={"id": "output"})

        return joined[list(exchanges.columns) + list(keep)]

    @staticmethod
    def production_exchanges_to_functions(
            nodes: pd.DataFrame,
            exchanges: pd.DataFrame,
            keep=()
    ) -> pd.DataFrame:
        """
        Joins exchanges of type `production` from the processes to the functions they belong to.
        """
        production_exchanges = exchanges.loc[exchanges["type"] == "production"]
        functions = nodes.dropna(subset="processor").drop("type", axis=1)

        joined = functions.merge(production_exchanges, left_on=["id", "processor"], right_on=["input", "output"])
        joined = joined.drop(["processor", "output"], axis=1)
        joined = joined.rename(columns={"id": "output"})

        return joined[list(exchanges.columns) + list(keep)]
