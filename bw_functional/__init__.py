__all__ = (
    "__version__",
    "allocation_before_writing",
    "allocation_strategies",
    "database_property_errors",
    "process_property_errors",
    "generic_allocation",
    "list_available_properties",
    "Process",
    "Function",
    "MFExchange",
    "MFExchanges",
    "FunctionalSQLiteDatabase",
    "property_allocation",
)

__version__ = "0.1"

from bw2data import labels
from bw2data.subclass_mapping import DATABASE_BACKEND_MAPPING, NODE_PROCESS_CLASS_MAPPING

from .allocation import allocation_strategies, generic_allocation, property_allocation
from .custom_allocation import (
    database_property_errors,
    process_property_errors,
    list_available_properties,
)
from .database import FunctionalSQLiteDatabase
from .node_classes import Process, Function
from .edge_classes import MFExchange, MFExchanges
from .node_dispatch import functional_node_dispatcher
from .utils import allocation_before_writing

DATABASE_BACKEND_MAPPING["functional_sqlite"] = FunctionalSQLiteDatabase
NODE_PROCESS_CLASS_MAPPING["functional_sqlite"] = functional_node_dispatcher


if "waste" not in labels.node_types:
    labels.lci_node_types.append("waste")
if "nonfunctional" not in labels.node_types:
    labels.other_node_types.append("nonfunctional")

