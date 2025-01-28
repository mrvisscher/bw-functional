from logging import getLogger

from bw2data import projects, databases, get_node
from bw2data.backends.proxies import Exchange, Exchanges
from bw2data.errors import UnknownObject

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

        if self["type"] == "production" and self.get("formula"):
            del self["formula"]
            raise NotImplementedError("Parameterization not supported for functions")

        super().save(signal, data_already_set, force_insert)

        process = self.output
        if isinstance(process, Process):
            process.save()

        if self["type"] == "production":
            process.allocate()

    def delete(self, signal: bool = True):
        from .node_classes import Process, Function
        log.debug(f"Deleting {self["type"]} Exchange: {self}")

        super().delete(signal)

        process = self.output
        if isinstance(process, Process):
            log.debug(f"Exchange has Process as output")
            process.save()

        if self["type"] == "production":
            try:
                function = self.input
                if isinstance(function, Function):
                    log.debug(f"Exchange has Product as input")
                    function.delete()
            except UnknownObject:
                log.warning("Function of production exchange not found")
                pass
            process.allocate()

