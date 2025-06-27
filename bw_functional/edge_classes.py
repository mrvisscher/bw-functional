from logging import getLogger
from copy import deepcopy

from bw2data import projects, databases
from bw2data.backends.proxies import Exchange, Exchanges, ExchangeDataset

log = getLogger(__name__)


class MFExchanges(Exchanges):
    """
    A specialized class for managing exchanges of multifunctional processes.

    This class extends the `Exchanges` class to provide additional functionality for
    deleting and iterating over exchanges in the context of multifunctional processes.
    """

    def delete(self, allow_in_sourced_project: bool = False):
        """
        Deletes all exchanges in the current collection.

        Delete exchanges by calling their methods instead of deleting them en masse. This enables enforcement of
        allocation rules.

        Args:
            allow_in_sourced_project (bool, optional): Whether to allow deletion in sourced projects.
                Defaults to False.

        Raises:
            NotImplementedError: If mass deletion is attempted in a sourced project without explicit permission.
        """
        if projects.dataset.is_sourced and not allow_in_sourced_project:
            raise NotImplementedError("Mass exchange deletion not supported in sourced projects")
        databases.set_dirty(self._key[0])
        for exchange in self:
            exchange.delete()

    def __iter__(self):
        """
        Iterates over the exchanges in the current collection.

        This method retrieves the queryset of exchanges and yields each one as an `MFExchange` object.

        Yields:
            MFExchange: An instance of the `MFExchange` class for each exchange in the collection.
        """
        for obj in self._get_queryset():
            yield MFExchange(obj)


class MFExchange(Exchange):
    """
    A class representing exchanges from the functional_sqlite backend.

    This class extends the `Exchange` class to provide additional functionality for handling
    multifunctional processes, including generating virtual edges, saving, and deleting exchanges.
    """

    @property
    def virtual_edges(self) -> list[dict]:
        """
        Generate virtual edges for the exchange.

        Virtual edges are created based on the allocation factors of the functions associated
        with the output process. For production exchanges, a single virtual edge is created
        where the output is set to the input.

        Returns:
            list[dict]: A list of dictionaries representing the virtual edges.

        Raises:
            ValueError: If the output is not an instance of the `Process` class.
        """
        from .node_classes import Process, Product
        edges = []

        if self["type"] == "production":
            ds = self.as_dict()
            ds["output"] = ds["input"]
            return [ds]

        if not isinstance(self.output, Process):
            raise ValueError("Output must be an instance of Process")

        for product in self.output.products():
            ds = deepcopy(self.as_dict())
            ds["amount"] = ds["amount"] * product.get("allocation_factor", 1)
            ds["output"] = product.key
            edges.append(ds)

        return edges

    def save(self, signal: bool = True, data_already_set: bool = False, force_insert: bool = False):
        """
        Save the exchange to the database.

        This method handles saving the exchange, ensuring that allocation rules are enforced
        for production exchanges. It also updates the associated process and function as needed.

        Args:
            signal (bool, optional): Whether to send a signal after saving. Defaults to True.
            data_already_set (bool, optional): Whether the data is already set. Defaults to False.
            force_insert (bool, optional): Whether to force an insert operation. Defaults to False.

        Raises:
            NotImplementedError: If parameterization is attempted for production exchanges.
        """
        from .node_classes import Process, Product
        log.debug(f"Saving {self['type']} Exchange: {self}")

        created = self.id is None  # the exchange is new if it has no id
        old = ExchangeDataset.get_by_id(self.id) if not created else None

        super().save(signal, data_already_set, force_insert)

        function = self.input
        process = self.output

        if not isinstance(process, Process) or not isinstance(function, Product):
            return

        if self["type"] == "production":
            if created:
                # If the exchange is new and production, the process has a new function

                # save the process to ensure the right type is set (multifunctional, process)
                process.save()

                # reallocate the process to update allocation factors based on the new set of functions
                process.allocate()

            # If the exchange is not new, we need to check if the amount has changed
            elif old.data["amount"] != self["amount"]:
                # If the amount has changed, we need to reallocate the process
                process.allocate()  # Includes function.save() for function type checking

    def delete(self, signal: bool = True):
        """
        Delete the exchange from the database.

        This method handles deleting the exchange and updates the associated process and function
        as needed. For production exchanges, it also deletes the associated function.

        Args:
            signal (bool, optional): Whether to send a signal after deletion. Defaults to True.
        """
        from .node_classes import Product, Process, MFActivity
        log.debug(f"Deleting {self['type']} Exchange: {self}")

        super().delete(signal)

        function = self.input
        process = self.output

        if not isinstance(process, Process) or not isinstance(function, Product):
            return

        if self["type"] == "production":
            # delete associated function through Class directly to avoid cascading delete
            MFActivity.delete(function)

            # save the process to ensure the right type is set (multifunctional, process, or nonfunctional)
            process.save()

            # reallocate the process to update allocation factors based on the remaining functions
            process.allocate()
