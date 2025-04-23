from typing import Optional, Union
from logging import getLogger

from bw2data import databases, get_node, labels
from bw2data.errors import UnknownObject, ValidityError
from bw2data.backends.proxies import Activity, ActivityDataset

from .edge_classes import MFExchanges, MFExchange
from .errors import NoAllocationNeeded

log = getLogger(__name__)


class MFActivity(Activity):
    """
    A class representing an activity of the functional_sqlite backend.

    This class extends the `Activity` class to provide additional functionality for managing
    multifunctional activities, including handling exchanges, technosphere, biosphere, and production flows. Subclasses
    methods mostly to make sure we're using the correct edge classes.
    """

    _edges_class = MFExchanges
    _edge_class = MFExchange

    def save(self, signal: bool = True, data_already_set: bool = False, force_insert: bool = False):
        """
        Save the activity to the database.

        This method logs the save operation and delegates the actual saving to the parent `Activity` class.

        Args:
            signal (bool, optional): Whether to send a signal after saving. Defaults to True.
            data_already_set (bool, optional): Whether the data is already set. Defaults to False.
            force_insert (bool, optional): Whether to force an insert operation. Defaults to False.
        """
        log.debug(f"Saving {self.__class__.__name__}: {self}")
        super().save(signal, data_already_set, force_insert)

    def delete(self, signal: bool = True):
        """
        Delete the activity from the database.

        This method logs the delete operation and delegates the actual deletion to the parent `Activity` class.

        Args:
            signal (bool, optional): Whether to send a signal after deletion. Defaults to True.
        """
        log.debug(f"Deleting {self.__class__.__name__}: {self}")
        super().delete(signal)

    @property
    def multifunctional(self) -> bool:
        """
        Check if the activity is multifunctional.

        Returns:
            bool: Always returns False, indicating the activity is not multifunctional by default.
        """
        return False

    def exchanges(self, exchanges_class=None, kinds=None, reverse=False):
        """
        Retrieve exchanges associated with the activity.

        Args:
            exchanges_class (type, optional): The class to use for exchanges. Defaults to None.
            kinds (list, optional): The types of exchanges to retrieve. Defaults to None.
            reverse (bool, optional): Whether to reverse the direction of the exchanges. Defaults to False.

        Returns:
            MFExchanges or exchanges_class: The exchanges associated with the activity.
        """
        if exchanges_class is None:
            return self._edges_class(self.key, kinds, reverse)
        return exchanges_class(self.key, kinds, reverse)

    def technosphere(self, exchanges_class=None):
        """
        Retrieve technosphere exchanges associated with the activity.

        Args:
            exchanges_class (type, optional): The class to use for exchanges. Defaults to None.

        Returns:
            MFExchanges or exchanges_class: The technosphere exchanges.
        """
        return self.exchanges(exchanges_class, kinds=labels.technosphere_negative_edge_types)

    def biosphere(self, exchanges_class=None):
        """
        Retrieve biosphere exchanges associated with the activity.

        Args:
            exchanges_class (type, optional): The class to use for exchanges. Defaults to None.

        Returns:
            MFExchanges or exchanges_class: The biosphere exchanges.
        """
        return self.exchanges(exchanges_class, kinds=labels.biosphere_edge_types)

    def production(self, include_substitution=False, exchanges_class=None):
        """
        Retrieve production exchanges associated with the activity.

        Args:
            include_substitution (bool, optional): Whether to include substitution exchanges. Defaults to False.
            exchanges_class (type, optional): The class to use for exchanges. Defaults to None.

        Returns:
            MFExchanges or exchanges_class: The production exchanges.
        """
        kinds = labels.technosphere_positive_edge_types
        if not include_substitution:
            kinds = [obj for obj in kinds if obj not in labels.substitution_edge_types]

        return self.exchanges(exchanges_class, kinds=kinds)

    def rp_exchange(self):
        """
        Retrieve the reference product exchange.

        Raises:
            NotImplementedError: This method is not implemented.
        """
        raise NotImplementedError

    def substitution(self, exchanges_class=None):
        """
        Retrieve substitution exchanges associated with the activity.

        Args:
            exchanges_class (type, optional): The class to use for exchanges. Defaults to None.

        Returns:
            MFExchanges or exchanges_class: The substitution exchanges.
        """
        return self.exchanges(exchanges_class, kinds=labels.substitution_edge_types)

    def upstream(self, kinds=labels.technosphere_negative_edge_types, exchanges_class=None) -> MFExchanges:
        """
        Retrieve upstream exchanges associated with the activity.

        Args:
            kinds (list, optional): The types of upstream exchanges to retrieve. Defaults to technosphere negative edge types.
            exchanges_class (type, optional): The class to use for exchanges. Defaults to None.

        Returns:
            MFExchanges: The upstream exchanges.
        """
        return self.exchanges(exchanges_class, kinds=kinds, reverse=True)

    def new_edge(self, **kwargs):
        """
        Create a new exchange linked to this activity.

        Args:
            **kwargs: Additional arguments for creating the exchange.

        Returns:
            MFExchange: The newly created exchange.
        """
        exc = super().new_edge(**kwargs)
        return self._edge_class(**exc)

    def copy(self, *args, **kwargs):
        """
        Create a copy of the activity.

        Args:
            *args: Positional arguments for the copy operation.
            **kwargs: Keyword arguments for the copy operation.

        Returns:
            MFActivity: A copy of the activity.
        """
        act = super().copy(*args, **kwargs)
        return self.__class__(document=act._document)


class Process(MFActivity):
    """
    A class representing a process in the functional_sqlite backend.

    This class extends the `MFActivity` class to provide additional functionality for managing processes,
    including creating new products, reductions, and handling allocation strategies.
    """

    def save(self, signal: bool = True, data_already_set: bool = False, force_insert: bool = False):
        """
        Save the process to the database.

        This method determines the type and allocation strategy of the process before saving it.
        If the allocation strategy changes, it triggers reallocation.

        Args:
            signal (bool, optional): Whether to send a signal after saving. Defaults to True.
            data_already_set (bool, optional): Whether the data is already set. Defaults to False.
            force_insert (bool, optional): Whether to force an insert operation. Defaults to False.
        """
        created = self.id is None
        old = ActivityDataset.get_by_id(self.id) if not created else None

        self["type"] = self.deduct_type()
        self["allocation"] = self.get("allocation", databases[self["database"]].get("default_allocation"))

        super().save(signal, data_already_set, force_insert)

        if not created and old.data.get("allocation") != self.get("allocation"):
            self.allocate()

    def copy(self, *args, **kwargs):
        """
        Create a copy of the process.

        Args:
            *args: Positional arguments for the copy operation.
            **kwargs: Keyword arguments for the copy operation.

        Returns:
            Process: A copy of the process.
        """
        act = super().copy(*args, **kwargs)

        for function in self.functions():
            input_database, input_code = function.key
            output_database, output_code = act.key

            MFExchange.ORMDataset.get(
                input_database=input_database, input_code=input_code, output_database=output_database,
                output_code=output_code, type="production").delete_instance()

            copied_fn = function.copy(processor=act.key, signal=False)
            copied_fn.create_processing_edge()
            copied_fn.save()

        return act

    def deduct_type(self) -> str:
        """
        Deduce the type of the process.

        Returns:
            str: The type of the process, which can be "multifunctional", "nonfunctional", or "process".
        """
        if self.multifunctional:
            return "multifunctional"
        elif not self.functional:
            return "nonfunctional"
        else:
            return "process"

    def new_product(self, **kwargs):
        """
        Create a new product associated with the process.

        Args:
            **kwargs: Additional arguments for creating the product.

        Returns:
            Function: A new product function.
        """
        kwargs["type"] = "product"
        kwargs["processor"] = self.key
        kwargs["database"] = self["database"]
        kwargs["properties"] = self.get("default_properties", {})
        return Function(**kwargs)

    def new_waste(self, **kwargs):
        """
        Create a new waste associated with the process.

        Args:
            **kwargs: Additional arguments for creating the reduction.

        Returns:
            Function: A new waste function.
        """
        kwargs["type"] = "waste"
        kwargs["processor"] = self.key
        kwargs["database"] = self["database"]
        kwargs["properties"] = self.get("default_properties", {})
        return Function(**kwargs)

    def new_default_property(self, name: str, unit: str, amount=1.0, normalize=False):
        """
        Add a new default property to the process and its associated functions.

        Args:
            name (str): The name of the property.
            unit (str): The unit of the property.
            amount (float, optional): The amount of the property. Defaults to 1.0.
            normalize (bool, optional): Whether to normalize the property. Defaults to False.

        Raises:
            ValueError: If the property already exists.
        """
        if name in self.get("properties", {}):
            raise ValueError(f"Property already exists within {self}")

        prop = {"unit": unit, "amount": amount, "normalize": normalize}

        self["default_properties"] = self.get("default_properties", {})
        self["default_properties"].update({name: prop})
        self.save()

        for function in self.functions():
            function["properties"] = function.get("properties", {})
            function["properties"].update({name: prop})
            function.save()

    def functions(self):
        """
        Retrieve the functions (products or wastes) associated with the process.

        Returns:
            list: A list of functions associated with the process.
        """
        excs = self.exchanges(kinds=["production"])
        return [exc.input for exc in excs]

    @property
    def functional(self) -> bool:
        """
        Check if the process is functional.

        Returns:
            bool: True if the process has at least one production exchange, False otherwise.
        """
        return len(self.production()) > 0

    @property
    def multifunctional(self) -> bool:
        """
        Check if the process is multifunctional.

        Returns:
            bool: True if the process has more than one production exchange, False otherwise.
        """
        return len(self.production()) > 1

    def allocate(self, strategy_label: Optional[str] = None) -> Union[None, NoAllocationNeeded]:
        """
        Allocate the process using the specified strategy.

        This method applies the allocation strategy to the process. If no strategy is provided,
        it uses the default allocation strategy from the process or database metadata.

        Args:
            strategy_label (str, optional): The label of the allocation strategy. Defaults to None.

        Returns:
            None or NoAllocationNeeded: Returns `NoAllocationNeeded` if allocation is skipped.

        Raises:
            ValueError: If no allocation strategy is found.
        """
        if self.get("skip_allocation") or not self.multifunctional:
            return NoAllocationNeeded()

        from . import allocation_strategies, property_allocation

        if strategy_label is None:
            if self.get("allocation"):
                strategy_label = self.get("allocation")
            else:
                strategy_label = databases[self["database"]].get("default_allocation")

        if not strategy_label:
            raise ValueError(
                "Can't find `default_allocation` in input arguments, or process/database metadata."
            )

        log.debug(f"Allocating {repr(self)} (id: {self.id}) with strategy {strategy_label}")

        alloc_function = allocation_strategies.get(strategy_label, property_allocation(strategy_label))
        alloc_function(self)


class Function(MFActivity):
    """
    Represents a function that can be either a 'product' or 'waste'.

    Functions should always have a `processor` key set, which is a process that handles the function.

    This class extends `MFActivity` and provides additional functionality for managing
    functions, including saving, deleting, and validating them, as well as handling
    processing edges and substitution.
    """

    def save(self, signal: bool = True, data_already_set: bool = False, force_insert: bool = False):
        """
        Save the function to the database.

        This method validates the function before saving, determines its type (product or waste),
        and handles changes to the processor, allocation properties, and substitution factors.

        Args:
            signal (bool, optional): Whether to send a signal after saving. Defaults to True.
            data_already_set (bool, optional): Whether the data is already set. Defaults to False.
            force_insert (bool, optional): Whether to force an insert operation. Defaults to False.

        Raises:
            ValidityError: If the function is not valid.
        """
        if not self.valid():
            raise ValidityError(
                "This activity can't be saved for the "
                + "following reasons\n\t* "
                + "\n\t* ".join(self.valid(why=True)[1])
            )

        created = self.id is None
        old = ActivityDataset.get_by_id(self.id) if not created else None

        if not created:
            self["type"] = self.deduct_type()  # make sure the type is set correctly

        super().save(signal, data_already_set, force_insert)

        edge = self.processing_edge

        # Check if the `processor` is the same as the one in the production edge otherwise update it
        if not created and edge.output != self["processor"]:
            log.info(f"Switching processor for {self}")
            edge.output = self["processor"]
            edge.save()

        # Check if the property used for allocation has changed and allocate if necessary
        if (not created and
                old.data.get("properties", {}).get(self.processor.get("allocation")) !=
                self.get("properties", {}).get(self.processor.get("allocation"))):
            self.processor.allocate()

        # Check if the substitution factor has changed and allocate if necessary
        if not created and (old.data.get("substitution_factor", 0) > 0) != (self.get("substitution_factor", 0) > 0):
            self.processor.allocate()

        # If the function is new and there's no production exchange yet, create one
        if created and not edge:
            self.create_processing_edge()

        # If the function is new and has a processing edge, allocate the processor
        if created and edge and isinstance(edge.output, Process):
            edge.output.allocate()

    def deduct_type(self) -> str:
        """
        Deduce the type of the function.

        Returns:
            str: The type of the function, which can be 'product', 'waste', or 'orphaned_product'.
        """
        edge = self.processing_edge
        if not edge:
            return "orphaned_product"
        elif edge.amount >= 0:
            return "product"
        elif edge.amount < 0:
            return "waste"

    def delete(self, signal: bool = True):
        """
        Delete the function and its upstream production exchanges.

        Args:
            signal (bool, optional): Whether to send a signal after deletion. Defaults to True.
        """
        # Delete the function by deleting it's production exchange. This will make sure there's no infinite loop
        self.upstream(["production"]).delete()

    @property
    def processing_edge(self) -> MFExchange | None:
        """
        Retrieve the processing edge of the function.

        Returns:
            MFExchange or None: The processing edge if it exists, otherwise None.

        Raises:
            ValidityError: If the function has multiple processing edges.
        """
        excs = self.exchanges(kinds=["production"], reverse=True)

        if len(excs) > 1:
            raise ValidityError("Invalid function has multiple processing edges")
        if len(excs) == 0:
            return None
        return list(excs)[0]

    def create_processing_edge(self):
        """
        Create a new processing edge for the function.
        """
        amount = 1.0 if self["type"] == "product" else -1.0
        MFExchange(input=self.key, output=self["processor"], amount=amount, type="production").save()

    @property
    def processor(self) -> Process | None:
        """
        Retrieve the processor (process) associated with the function. If no processor key is set, will try to deduct
        the processor from the production edge and set the processor key afterwards.

        Returns:
            Process or None: The associated process, or None if not found.
        """
        if key := self.get("processor"):
            return get_node(key=key)

        edge = self.processing_edge
        if not edge:
            return None

        processor = edge.output
        self["processor"] = processor.key
        return processor

    @property
    def virtual_edges(self) -> list[dict]:
        """
        Generate virtual edges for the function.

        Virtual edges are created based on the allocation factor and include technosphere,
        biosphere, and production exchanges.

        Returns:
            list[dict]: A list of dictionaries representing the virtual edges.
        """
        virtual_exchanges = []
        for exchange in self._edges_class(self["processor"], ["technosphere", "biosphere"]):
            ds = exchange.as_dict()
            ds["amount"] = ds["amount"] * self.get("allocation_factor", 1)
            ds["output"] = self.key
            virtual_exchanges.append(ds)

        production = self.processing_edge.as_dict()
        production["output"] = self.key

        return virtual_exchanges

    def substitute(self, substitute_key: tuple | None = None, substitution_factor=1.0):
        """
        Set or remove substitution for the function.

        Args:
            substitute_key (tuple, optional): The key of the substitute. Defaults to None.
            substitution_factor (float, optional): The substitution factor. Defaults to 1.0.
        """
        if substitute_key is None:
            if self.get("substitute"):
                del self["substitute"]
            if self.get("substitution_factor"):
                del self["substitution_factor"]
            return

        self["substitute"] = substitute_key
        self["substitution_factor"] = substitution_factor

    def new_edge(self, **kwargs):
        """
        Create a new edge for the function.

        Raises:
            NotImplementedError: Functions cannot have input edges.
        """
        raise NotImplementedError("Functions cannot have input edges")

    def valid(self, why=False):
        """
        Validate the function.

        A `Function` is considered valid if:
        - It has a `processor` key that is a tuple and corresponds to an existing process node.
        - It has a `type` field, which must be either "product" or "waste".
        - It passes the validation checks of the parent `MFActivity` class.

        Args:
            why (bool, optional): Whether to return the reasons for invalidity. Defaults to False.

        Returns:
            bool or tuple: True if valid, otherwise False or a tuple with reasons for invalidity.
        """
        if super().valid():
            errors = []
        else:
            _, errors = super().valid(why=True)

        if not self.get("processor") and not self.processor:
            errors.append("Missing field ``processor``")
        elif not isinstance(self["processor"], tuple):
            errors.append("Field ``processor`` must be a tuple")
        else:
            try:
                get_node(key=self.get("processor"))
            except UnknownObject:
                errors.append("Processor node not found")

        if not self.get("type"):
            errors.append("Missing field ``type``, function most be ``product`` or ``waste``")
        elif self["type"] not in ["product", "waste", "orphaned_product"]:
            errors.append("Function ``type`` most be ``product`` or ``waste``")

        if errors:
            if why:
                return (False, errors)
            else:
                return False
        else:
            return True

