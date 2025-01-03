__all__ = (
    "__version__",
    "add_custom_property_allocation_to_project",
    "allocation_before_writing",
    "allocation_strategies",
    "database_property_errors",
    "process_property_errors",
    "generic_allocation",
    "list_available_properties",
    "Process",
    "Function",
    "FunctionalSQLiteDatabase",
    "property_allocation",
    "ReadOnlyProcess",
)

__version__ = "0.1"

# # Follows guidance from https://loguru.readthedocs.io/en/stable/resources/recipes.html#configuring-loguru-to-be-used-by-a-library-or-an-application
# # For development or to get more detail on what is really happening, re-enable with:
# # logger.enable("multifunctional")
# from loguru import logger
#
# logger.disable("multifunctional")

from bw2data import labels
from bw2data.subclass_mapping import DATABASE_BACKEND_MAPPING, NODE_PROCESS_CLASS_MAPPING

from .allocation import allocation_strategies, generic_allocation, property_allocation
from .custom_allocation import (
    add_custom_property_allocation_to_project,
    database_property_errors,
    process_property_errors,
    list_available_properties,
)
from .database import FunctionalSQLiteDatabase
from .node_classes import Process, Function, ReadOnlyProcess
from .node_dispatch import functional_node_dispatcher
from .utils import allocation_before_writing

DATABASE_BACKEND_MAPPING["functional_sqlite"] = FunctionalSQLiteDatabase
NODE_PROCESS_CLASS_MAPPING["functional_sqlite"] = functional_node_dispatcher


if "readonly_process" not in labels.process_node_types:
    labels.process_node_types.append("readonly_process")
if "readonly_process" not in labels.node_types:
    labels.lci_node_types.append("readonly_process")
    labels.lci_node_types.append("reduct")
    labels.lci_node_types.append("nonfunctional")

if "reduction" not in labels.technosphere_negative_edge_types:
    labels.technosphere_negative_edge_types.append("reduction")
