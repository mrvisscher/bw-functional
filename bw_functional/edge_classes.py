from logging import getLogger
from copy import deepcopy

from bw2data import projects, databases
from bw2data.backends.proxies import Exchange, Exchanges, ExchangeDataset

log = getLogger(__name__)


class MFExchanges(Exchanges):
    def delete(self, allow_in_sourced_project: bool = False):
        if projects.dataset.is_sourced and not allow_in_sourced_project:
            raise NotImplementedError("Mass exchange deletion not supported in sourced projects")
        databases.set_dirty(self._key[0])
        for exchange in self:
            exchange.delete()

    def __iter__(self):
        for obj in self._get_queryset():
            yield MFExchange(obj)


class MFExchange(Exchange):

    @property
    def virtual_edges(self) -> list[dict]:
        from .node_classes import Process, Function
        edges = []

        if self["type"] == "production":
            ds = self.as_dict()
            ds["output"] = ds["input"]
            return [ds]

        if not isinstance(self.output, Process):
            raise ValueError("Output must be an instance of Process")

        for function in self.output.functions():
            ds = deepcopy(self.as_dict())
            ds["amount"] = ds["amount"] * function.get("allocation_factor", 1)
            ds["output"] = function.key
            edges.append(ds)

        return edges

    def save(self, signal: bool = True, data_already_set: bool = False, force_insert: bool = False):
        from .node_classes import Process, Function
        log.debug(f"Saving {self['type']} Exchange: {self}")

        created = self.id is None
        old = ExchangeDataset.get_by_id(self.id) if not created else None

        # no support for parameterization at this time because we can't allocate when amounts change through parameter
        # changes as this happens behind the scenes
        if self["type"] == "production" and "formula" in self:
            del self["formula"]
            raise NotImplementedError("Parameterization not supported for functions")

        super().save(signal, data_already_set, force_insert)

        function = self.input
        process = self.output

        if not isinstance(process, Process) or not isinstance(function, Function):
            return

        if self["type"] == "production":
            if created:
                process.save()
                process.allocate()
            elif old.data["amount"] != self["amount"]:
                process.allocate()  # includes function.save() for function type checking

    def delete(self, signal: bool = True):
        from .node_classes import Function, Process, MFActivity
        log.debug(f"Deleting {self['type']} Exchange: {self}")

        super().delete(signal)

        function = self.input
        process = self.output

        if not isinstance(process, Process) or not isinstance(function, Function):
            return

        if self["type"] == "production":
            MFActivity.delete(function)
            process.save()
            process.allocate()

