from bw2data import projects, databases, get_node
from bw2data.backends.proxies import Exchange, Exchanges


class MFExchanges(Exchanges):
    def delete(self, allow_in_sourced_project: bool = False, delete_function=True):
        if projects.dataset.is_sourced and not allow_in_sourced_project:
            raise NotImplementedError("Mass exchange deletion not supported in sourced projects")
        databases.set_dirty(self._key[0])
        for exchange in self:
            exchange.delete(delete_function=delete_function)

    def __iter__(self):
        for obj in self._get_queryset():
            yield MFExchange(obj)


class MFExchange(Exchange):
    def save(self, signal: bool = True, data_already_set: bool = False, force_insert: bool = False):
        from .node_classes import Process

        super().save(signal, data_already_set, force_insert)

        process = get_node(key=self["output"])
        if isinstance(process, Process):
            process.save()

    def delete(self, signal: bool = True, delete_function: bool = True):
        from .node_classes import Process, Function

        super().delete(signal)

        process = get_node(key=self["output"])
        if isinstance(process, Process):
            process.save()

        if delete_function:
            function = get_node(key=self["input"])
            if self["type"] in ["production", "reduction"] and isinstance(function, Function):
                function.delete()


class ReadOnlyExchange(MFExchange):
    def save(self):
        raise NotImplementedError("Read-only exchange")

    def delete(self):
        raise NotImplementedError("Read-only exchange")

    def _set_output(self, value):
        raise NotImplementedError("Read-only exchange")

    def _set_input(self, value):
        raise NotImplementedError("Read-only exchange")

    def __setitem__(self, key, value):
        raise NotImplementedError("Read-only exchange")


class ReadOnlyExchanges(MFExchanges):
    def __iter__(self):
        for obj in self._get_queryset():
            yield ReadOnlyExchange(obj)
