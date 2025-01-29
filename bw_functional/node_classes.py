from typing import Optional, Union
from logging import getLogger

from bw2data import databases, get_node, labels
from bw2data.errors import UnknownObject, ValidityError
from bw2data.backends.proxies import Activity, Exchanges, Exchange

from .edge_classes import MFExchanges, MFExchange
from .errors import NoAllocationNeeded

log = getLogger(__name__)


class MFActivity(Activity):
    _edges_class = MFExchanges
    _edge_class = MFExchange

    def save(self, signal: bool = True, data_already_set: bool = False, force_insert: bool = False):
        log.debug(f"Saving {self.__class__.__name__}: {self}")
        super().save(signal, data_already_set, force_insert)

    def delete(self, signal: bool = True):
        log.debug(f"Deleting {self.__class__.__name__}: {self}")
        super().delete(signal)

    @property
    def multifunctional(self) -> bool:
        return False

    def exchanges(self, exchanges_class=None, kinds=None, reverse=False):
        if exchanges_class is None:
            return self._edges_class(self.key, kinds, reverse)
        return exchanges_class(self.key, kinds, reverse)

    def technosphere(self, exchanges_class=None):
        return self.exchanges(exchanges_class, kinds=labels.technosphere_negative_edge_types)

    def biosphere(self, exchanges_class=None):
        return self.exchanges(exchanges_class, kinds=labels.biosphere_edge_types)

    def production(self, include_substitution=False, exchanges_class=None):
        kinds = labels.technosphere_positive_edge_types
        if not include_substitution:
            kinds = [obj for obj in kinds if obj not in labels.substitution_edge_types]

        return self.exchanges(exchanges_class, kinds=kinds)

    def rp_exchange(self):
        raise NotImplementedError

    def substitution(self, exchanges_class=None):
        return self.exchanges(exchanges_class, kinds=labels.substitution_edge_types)

    def upstream(self, kinds=labels.technosphere_negative_edge_types, exchanges_class=None) -> MFExchanges:
        return self.exchanges(exchanges_class, kinds=kinds, reverse=True)

    def new_edge(self, **kwargs):
        """Create a new exchange linked to this activity"""
        exc = super().new_edge(**kwargs)
        return self._edge_class(**exc)

    def copy(self, *args, **kwargs):
        act = super().copy(*args, **kwargs)
        return self.__class__(**act)


class Process(MFActivity):

    def save(self, signal: bool = True, data_already_set: bool = False, force_insert: bool = False):
        self["type"] = self.deduct_type()
        super().save(signal, data_already_set, force_insert)

    def deduct_type(self) -> str:
        if self.multifunctional:
            return "multifunctional"
        elif not self.functional:
            return "nonfunctional"
        else:
            return "process"

    def new_product(self, **kwargs):
        kwargs["type"] = "product"
        kwargs["processor"] = self.key
        kwargs["database"] = self["database"]
        kwargs["properties"] = self.get("default_properties", {})
        return Function(**kwargs)

    def new_reduct(self, **kwargs):
        kwargs["type"] = "waste"
        kwargs["processor"] = self.key
        kwargs["database"] = self["database"]
        kwargs["properties"] = self.get("default_properties", {})
        return Function(**kwargs)

    def new_default_property(self, name: str, unit: str, amount=1.0, normalize=False):
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
        excs = self.exchanges(kinds=["production", "reduction"])
        return [exc.input for exc in excs]

    @property
    def functional(self) -> bool:
        return len(self.production()) > 0

    @property
    def multifunctional(self) -> bool:
        return len(self.production()) > 1

    def allocate(self, strategy_label: Optional[str] = None) -> Union[None, NoAllocationNeeded]:
        if self.get("skip_allocation") or not self.multifunctional:
            return NoAllocationNeeded()

        from . import allocation_strategies, property_allocation

        if strategy_label is None:
            if self.get("default_allocation"):
                strategy_label = self.get("default_allocation")
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
    """Can be type 'product' or 'waste'"""

    def save(self, signal: bool = True, data_already_set: bool = False, force_insert: bool = False):
        if not self.valid():
            raise ValidityError(
                "This activity can't be saved for the "
                + "following reasons\n\t* "
                + "\n\t* ".join(self.valid(why=True)[1])
            )

        edge = self.processing_edge

        if edge and edge.output != self.get("processor"):
            print(f"Switching processor for {self}")
            edge.delete()
            edge = None

        if not edge:
            amount = 1.0 if self["type"] == "product" else -1.0
            exc = Exchange(input=self.key, output=self.get("processor"), amount=amount, functional=True, type="production")
            exc.save()
            exc.output.save()

        self["type"] = self.deduct_type()

        super().save(signal, data_already_set, force_insert)

    def deduct_type(self) -> str:
        edge = self.processing_edge
        if not edge:
            return "orphaned_product"
        elif edge.amount >= 0:
            return "product"
        elif edge.amount < 0:
            return "waste"

    def delete(self, signal: bool = True):
        self.upstream(["production"]).delete()

    @property
    def processing_edge(self) -> MFExchange | None:
        excs = self.exchanges(kinds=["production", "reduction"], reverse=True)
        excs = [exc for exc in excs if exc.output.get("type") != "readonly_process"]

        if len(excs) > 1:
            raise ValidityError("Invalid function has multiple processing edges")
        if len(excs) == 0:
            return None
        return excs[0]

    @property
    def processor(self) -> Process | None:
        """Return the single process with the production/reduction flow"""
        if key := self.get("processor"):
            return get_node(key=key)

        edge = self.processing_edge
        if not edge:
            return None

        processor = edge.output
        self["processor"] = processor.key
        return processor

    def substitute(self):
        """Can I think of a way to substitute here?"""
        pass

    def new_edge(self, **kwargs):
        """Impossible for a Function"""
        raise NotImplementedError("Functions cannot have input edges")

    def valid(self, why=False):
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
        elif self["type"] != "product" and self["type"] != "waste":
            errors.append("Function ``type`` most be ``product`` or ``waste``")

        if errors:
            if why:
                return (False, errors)
            else:
                return False
        else:
            return True

