import sqlite3
import pickle
import datetime
from logging import getLogger
from time import time

import numpy as np
import pandas as pd
from fsspec.implementations.zip import ZipFileSystem

import stats_arrays as sa

from bw_processing import clean_datapackage_name, create_datapackage
from bw2data.backends import SQLiteBackend, sqlite3_lci_db
from bw2data.backends.schema import ActivityDataset
from pandas._libs.missing import NAType

from .node_classes import Process, Product

log = getLogger(__name__)

UNCERTAINTY_FIELDS = ["uncertainty_type", "loc", "scale", "shape", "minimum", "maximum"]


class FunctionalSQLiteDatabase(SQLiteBackend):
    """
    A specialized SQLite backend for handling functional databases.

    This class extends the `SQLiteBackend` to provide additional functionality for
    processing and managing processes with one or more functions, including relabeling data, registering
    metadata, and processing data into a structured format..
    """

    backend = "functional_sqlite"

    @staticmethod
    def node_class(document: ActivityDataset = None) -> Process | Product:
        """Dispatch the correct node class depending on document attributes."""
        if document and document.type in ["product", "waste"]:
            return Product(document=document)
        else:
            return Process(document=document)

    @staticmethod
    def relabel_data(data: dict, old_name: str, new_name: str) -> dict:
        """
        Relabels data to update references from an old database name to a new one.

        This method updates the `input`, `output`, and `processor` fields in the data
        dictionary to reflect the new database name.

        Args:
            data (dict): The data dictionary containing activity and exchange information.
            old_name (str): The old database name to be replaced.
            new_name (str): The new database name to replace the old one.

        Returns:
            dict: A dictionary with updated references to the new database name.
        """

        def relabel_exchanges(obj: dict, old_name: str, new_name: str) -> dict:
            """
            Updates the `input`, `output`, and `processor` fields in an exchange object.

            Args:
                obj (dict): The exchange object to update.
                old_name (str): The old database name to be replaced.
                new_name (str): The new database name to replace the old one.

            Returns:
                dict: The updated exchange object.
            """
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
        """
        Registers the database with default metadata.

        This method ensures that the `default_allocation` key is set to "equal" if not
        provided in the keyword arguments, and then calls the parent class's `register` method.

        Args:
            **kwargs: Additional metadata to register with the database.
        """
        if "default_allocation" not in kwargs:
            kwargs["default_allocation"] = "equal"
        super().register(**kwargs)

    def process(self, csv: bool = False, allocate: bool = True) -> None:
        """
        Processes the database to generate structured data tables and metadata.

        This method retrieves data from the database, processes it into technosphere
        and biosphere matrices, and serializes the results into a datapackage.

        Args:
            csv (bool, optional): Whether to output the results as CSV files. Defaults to False.
            allocate (bool, optional): Whether to perform allocation during processing. Defaults to True.
        """
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
        """
        Retrieves and processes data tables from the SQLite database.

        This method extracts node and exchange data from the database, maps IDs to
        their corresponding keys, and identifies dependent databases.

        Returns:
            tuple: A tuple containing:
                - pd.DataFrame: The node data.
                - pd.DataFrame: The exchange data.
                - set: A set of dependent database names.
        """
        t = time()
        con = sqlite3.connect(sqlite3_lci_db._filepath)

        def id_mapper(key) -> NAType | int:
            """
            Maps a key to its corresponding ID.

            Args:
                key: The key to map.

            Returns:
                NAType | int: The mapped ID or NA if the key is invalid.

            Raises:
                KeyError: If the key is not found in the mapping dictionary.
            """
            if not isinstance(key, tuple):
                return pd.NA
            try:
                return id_map_dict[key]
            except KeyError:
                raise KeyError(f"Node key {key} not found.")

        # Retrieve the mapping of IDs to database and code from the `activitydataset` table
        id_map = pd.read_sql(f"SELECT id, database, code FROM activitydataset", con)

        # Create a new column `key` by combining `database` and `code` into a tuple
        id_map["key"] = id_map.loc[:, ["database", "code"]].apply(tuple, axis=1)

        # Convert the mapping of `key` to `id` into a dictionary for quick lookups
        id_map_dict = id_map.set_index("key")["id"].to_dict()

        # Retrieve raw data from the `activitydataset` table for the current database
        raw = pd.read_sql(f"SELECT data FROM activitydataset WHERE database = '{self.name}'", con)

        # Deserialize the raw data using `pickle` and create a DataFrame with specified columns
        node_df = pd.DataFrame([pickle.loads(x) for x in raw["data"]],
                               columns=["database", "code", "type", "processor", "allocation_factor", "substitute",
                                        "substitution_factor"])

        # Merge the `node_df` with the `id_map` to include the `id` column
        node_df = node_df.merge(id_map[["database", "code", "id"]], on=["database", "code"])

        # Map the `processor` and `substitute` columns to their corresponding IDs using `id_mapper`
        node_df["processor"] = node_df["processor"].map(id_mapper).astype("Int64")
        node_df["substitute"] = node_df["substitute"].map(id_mapper).astype("Int64")

        # Select and reorder the relevant columns for the final `node_df`
        node_df = node_df[["id", "type", "processor", "allocation_factor", "substitute", "substitution_factor"]]

        # Retrieve raw data from the `exchangedataset` table for the current database
        raw = pd.read_sql(f"SELECT data, input_database FROM exchangedataset WHERE output_database = '{self.name}'",
                          con)

        # Deserialize the raw data using `pickle` and create a DataFrame with specified columns
        exc_df = pd.DataFrame([pickle.loads(x) for x in raw["data"]],
                              columns=["input", "output", "type", "amount", "uncertainty type"] + UNCERTAINTY_FIELDS)

        # Update the `uncertainty_type` column to ensure consistency
        exc_df.update(exc_df["uncertainty_type"].rename("uncertainty type"))
        exc_df["uncertainty_type"] = exc_df["uncertainty type"]

        # Drop the redundant `uncertainty type` column
        exc_df.drop(["uncertainty type"], axis=1, inplace=True)

        # Map the `input` and `output` columns to their corresponding IDs using `id_mapper`
        exc_df["input"] = exc_df["input"].map(id_mapper).astype("Int64")
        exc_df["output"] = exc_df["output"].map(id_mapper).astype("Int64")

        # Identify dependent databases by extracting unique values from the `input_database` column
        dependents = set(raw["input_database"].unique())

        # Remove the current database name from the set of dependents, if present
        if self.name in dependents:
            dependents.remove(self.name)

        # Close the SQLite connection
        con.close()

        log.debug(f"Processing: built tables from SQL in {time() - t:.2f} seconds")

        return node_df, exc_df, dependents


class Build:
    """
    A utility class for constructing technosphere and biosphere matrices from nodes and exchanges.

    This class provides static methods to process and allocate data into structured formats
    for use in life cycle assessment (LCA) models. It includes methods for handling technosphere
    and biosphere exchanges, as well as consumption and production flows.
    """

    @staticmethod
    def technosphere(nodes, exchanges):
        """
        Constructs the technosphere matrix by combining consumption and production flows.

        Args:
            nodes (pd.DataFrame): The dataframe containing node data.
            exchanges (pd.DataFrame): The dataframe containing exchange data.

        Returns:
            pd.DataFrame: A concatenated dataframe of consumption and production flows.
        """
        consumption = Build.consumption(nodes, exchanges)
        production = Build.production(nodes, exchanges)
        return pd.concat([consumption, production])

    @staticmethod
    def biosphere(nodes, exchanges):
        """
        Constructs the biosphere matrix by allocating biosphere exchanges.

        Args:
            nodes (pd.DataFrame): The dataframe containing node data.
            exchanges (pd.DataFrame): The dataframe containing exchange data.

        Returns:
            pd.DataFrame: A dataframe containing allocated biosphere exchanges with additional metadata.
        """
        x = Build.allocated(nodes, exchanges, ["biosphere"])
        x["flip"] = False
        return x[["row", "col", "amount", "flip"] + UNCERTAINTY_FIELDS]

    @staticmethod
    def consumption(nodes, exchanges):
        """
        Constructs the consumption flows by allocating technosphere exchanges.

        Args:
            nodes (pd.DataFrame): The dataframe containing node data.
            exchanges (pd.DataFrame): The dataframe containing exchange data.

        Returns:
            pd.DataFrame: A dataframe containing allocated consumption flows with additional metadata.
        """
        x = Build.allocated(nodes, exchanges, ["technosphere"])
        x["flip"] = True
        return x[["row", "col", "amount", "flip"] + UNCERTAINTY_FIELDS]

    @staticmethod
    def production(nodes, exchanges):
        """
        Constructs the production flows by joining production exchanges to their respective functions.

        Args:
            nodes (pd.DataFrame): The dataframe containing node data.
            exchanges (pd.DataFrame): The dataframe containing exchange data.

        Returns:
            pd.DataFrame: A dataframe containing production flows with additional metadata.
        """
        x = Join.production_exchanges_to_functions(nodes, exchanges)

        x["flip"] = False
        x.rename(columns={"input": "row", "output": "col"}, inplace=True)

        return x[["row", "col", "amount", "flip"] + UNCERTAINTY_FIELDS]

    @staticmethod
    def allocated(nodes, exchanges, exchange_types):
        """
        Allocates exchanges of specified types to their respective functions.

        Args:
            nodes (pd.DataFrame): The dataframe containing node data.
            exchanges (pd.DataFrame): The dataframe containing exchange data.
            exchange_types (list or tuple): The types of exchanges to allocate (e.g., "biosphere", "technosphere").

        Returns:
            pd.DataFrame: A dataframe containing allocated exchanges with additional metadata.
        """
        x = Join.exchanges_to_functions(nodes, exchanges, exchange_types)
        x = Mutate.allocate_amount(x)
        x = Mutate.allocate_distributions(x)
        x.rename(columns={"input": "row", "output": "col"}, inplace=True)

        return x[["row", "col", "amount"] + UNCERTAINTY_FIELDS]


class Mutate:
    """
    A utility class for mutating dataframes in the `bw_functional` framework.
    This class provides methods to allocate amounts and distributions,
    and set default uncertainty values for exchanges.
    It is used to process and modify dataframes containing exchange and node information.
    """
    @staticmethod
    def allocate_amount(df: pd.DataFrame) -> pd.DataFrame:
        """
        Allocate amounts for non-functional exchanges.

        This method multiplies the `amount` column by the `allocation_factor` column for each row in the dataframe.
        If the `allocation_factor` is not defined (NaN), it defaults to 1.

        Args:
            df (pd.DataFrame): A dataframe containing at least `amount` and `allocation_factor` columns.

        Returns:
            pd.DataFrame: The updated dataframe with allocated amounts in the `amount` column.
        """
        df["amount"] = df["allocation_factor"].fillna(1) * df["amount"]
        return df

    @staticmethod
    def allocate_distributions(df: pd.DataFrame) -> pd.DataFrame:
        """
        Allocate uncertainty distributions for process-functions.

        This method adjusts uncertainty distribution parameters (e.g., `loc`, `scale`, `minimum`, `maximum`)
        based on the `allocation_factor`. It supports standard distributions and lognormal distributions.
        Unsupported distributions with non-default allocation factors will trigger a warning.

        Args:
            df (pd.DataFrame): A dataframe containing uncertainty columns (`loc`, `scale`, etc.) and
                an `allocation_factor` column.

        Returns:
            pd.DataFrame: The updated dataframe with allocated uncertainty distributions.

        Notes:
            - Standard distributions are adjusted by multiplying their parameters by the `allocation_factor`.
            - Lognormal distributions adjust the `loc` parameter by adding the natural logarithm of the
              `allocation_factor`.
            - Unsupported distributions include Bernoulli, Discrete Uniform, Beta, and Student's T.
        """
        # Distributions that use the standard method
        standard = [sa.NormalUncertainty.id, sa.UniformUncertainty.id, sa.TriangularUncertainty.id,
                    sa.WeibullUncertainty.id, sa.GammaUncertainty.id, sa.GeneralizedExtremeValueUncertainty.id,
                    sa.UndefinedUncertainty, sa.NoUncertainty]
        # Lognormal uncertainty
        ln = [sa.LognormalUncertainty.id]

        # Identify unsupported distributions with non-default allocation factors
        labels = (
            (~df["uncertainty_type"].isin(standard + ln)) &
            (df["allocation_factor"].fillna(1) != 1).any(axis=None)
        )
        if pd.Series.any(labels):
            log.warning("Database contains distributions that cannot be allocated")

        # Apply the standard method to applicable distributions
        labels = df["uncertainty_type"].isin(standard)
        df.loc[labels, "loc"] = df.loc[labels, "loc"] * df.loc[labels, "allocation_factor"].fillna(1)
        df.loc[labels, "scale"] = df.loc[labels, "scale"] * df.loc[labels, "allocation_factor"].fillna(1)
        df.loc[labels, "minimum"] = df.loc[labels, "minimum"] * df.loc[labels, "allocation_factor"].fillna(1)
        df.loc[labels, "maximum"] = df.loc[labels, "maximum"] * df.loc[labels, "allocation_factor"].fillna(1)

        # Apply the lognormal method to lognormal distributions
        labels = df["uncertainty_type"].isin(ln)
        df.loc[labels, "loc"] = df.loc[labels, "loc"] + np.log(df.loc[labels, "allocation_factor"].fillna(1))

        return df

    @staticmethod
    def set_default_uncertainty_values(df: pd.DataFrame) -> pd.DataFrame:
        """
        Set default uncertainty values for exchanges.

        This method ensures that all exchanges have a valid uncertainty type. For exchanges without
        defined uncertainty, it sets the type to `UndefinedUncertainty` and assigns the `amount` value
        to the `loc` parameter.

        Args:
            df (pd.DataFrame): A dataframe containing uncertainty columns (`uncertainty_type`, `loc`, etc.)
                and an `amount` column.

        Returns:
            pd.DataFrame: The updated dataframe with default uncertainty values set.
        """
        # Set all undefined uncertainties to the UndefinedUncertainty type
        df["uncertainty_type"] = df["uncertainty_type"].fillna(sa.UndefinedUncertainty.id)

        # Identify rows with UndefinedUncertainty or NoUncertainty and missing `loc`
        labels = (
            df["uncertainty_type"].isin([sa.UndefinedUncertainty.id, sa.NoUncertainty.id]) &
            (df["loc"].isna())
        )
        # Replace `loc` with the exchange amount
        df.loc[labels, "loc"] = df.loc[labels, "amount"]
        return df


class Join:
    """
    Utility class for joining nodes and exchanges in the `bw_functional` framework.

    This class provides methods to create a square matrix by using function IDs as row and column indexes.
    Since all exchanges of a process must be bound to its functions, the methods in this class join the
    `nodes` and `exchanges` dataframes based on the `processor` of the node and the `output` of the exchange.
    """

    @staticmethod
    def exchanges_to_functions(
            nodes: pd.DataFrame,
            exchanges: pd.DataFrame,
            exchange_types: tuple | list,
            keep=("allocation_factor",)
    ) -> pd.DataFrame:
        """
        Joins exchanges of specified types from processes to all the functions of the processes.

        This method filters exchanges by their type, then joins them with the functions of the processes
        based on the `processor` field in the `nodes` dataframe and the `output` field in the `exchanges` dataframe.

        Args:
            nodes (pd.DataFrame): The dataframe containing node data, including the `processor` field.
            exchanges (pd.DataFrame): The dataframe containing exchange data, including the `type` and `output` fields.
            exchange_types (tuple | list): The types of exchanges to include in the join (e.g., "biosphere", "technosphere").
            keep (tuple, optional): Additional columns from the `exchanges` dataframe to retain in the result. Defaults to ("allocation_factor",).

        Returns:
            pd.DataFrame: A dataframe containing the joined data, with the `id` column from `nodes` renamed to `output`.
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
        Joins production exchanges from processes to the functions they belong to.

        This method filters exchanges of type `production`, then joins them with the functions of the processes
        based on the `id` and `processor` fields in the `nodes` dataframe and the `input` and `output` fields
        in the `exchanges` dataframe.

        Args:
            nodes (pd.DataFrame): The dataframe containing node data, including the `id` and `processor` fields.
            exchanges (pd.DataFrame): The dataframe containing exchange data, including the `type`, `input`, and `output` fields.
            keep (tuple, optional): Additional columns from the `exchanges` dataframe to retain in the result. Defaults to an empty tuple.

        Returns:
            pd.DataFrame: A dataframe containing the joined data, with the `id` column from `nodes` renamed to `output`.
        """
        production_exchanges = exchanges.loc[exchanges["type"] == "production"]
        functions = nodes.dropna(subset="processor").drop("type", axis=1)

        joined = functions.merge(production_exchanges, left_on=["id", "processor"], right_on=["input", "output"])
        joined = joined.drop(["processor", "output"], axis=1)
        joined = joined.rename(columns={"id": "output"})

        return joined[list(exchanges.columns) + list(keep)]
