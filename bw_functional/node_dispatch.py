from typing import Optional

from bw2data.backends.proxies import Activity
from bw2data.backends.schema import ActivityDataset

from .node_classes import Process, Function, ReadOnlyProcess


def functional_node_dispatcher(node_obj: Optional[ActivityDataset] = None) -> Activity:
    """Dispatch the correct node class depending on node_obj attributes."""
    if node_obj and node_obj.type == "readonly_process":
        return ReadOnlyProcess(document=node_obj)
    elif node_obj and node_obj.type in ["product", "reduct"]:
        return Function(document=node_obj)
    else:
        return Process(document=node_obj)
