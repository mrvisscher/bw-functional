from logging import getLogger

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
    def save(self, signal: bool = True, data_already_set: bool = False, force_insert: bool = False):
        from .node_classes import Process, Function
        log.debug(f"Saving {self["type"]} Exchange: {self}")

        created = self.id is None

        if self["type"] == "production" and self.get("formula"):
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
            elif ExchangeDataset.get_by_id(self.id).data["amount"] != self["amount"]:
                process.allocate()  # includes function.save() for function type checking


    def delete(self, signal: bool = True):
        from .node_classes import Function, Process, MFActivity
        log.debug(f"Deleting {self["type"]} Exchange: {self}")

        super().delete(signal)

        function = self.input
        process = self.output

        if not isinstance(process, Process) or not isinstance(function, Function):
            return

        if self["type"] == "production":
            MFActivity.delete(function)
            process.save()
            process.allocate()

