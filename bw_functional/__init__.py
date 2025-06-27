__all__ = (
    "__version__",
    "allocation_strategies",
    "database_property_errors",
    "process_property_errors",
    "generic_allocation",
    "list_available_properties",
    "Process",
    "Product",
    "MFExchange",
    "MFExchanges",
    "FunctionalSQLiteDatabase",
    "property_allocation",
)

__version__ = "0.1"

from logging import getLogger

from bw2data import labels
from bw2data.subclass_mapping import DATABASE_BACKEND_MAPPING, NODE_PROCESS_CLASS_MAPPING

from .allocation import allocation_strategies, generic_allocation, property_allocation
from .custom_allocation import (
    database_property_errors,
    process_property_errors,
    list_available_properties,
)
from .database import FunctionalSQLiteDatabase
from .node_classes import Process, Product
from .edge_classes import MFExchange, MFExchanges
from .node_dispatch import functional_node_dispatcher

log = getLogger(__name__)

DATABASE_BACKEND_MAPPING["functional_sqlite"] = FunctionalSQLiteDatabase
NODE_PROCESS_CLASS_MAPPING["functional_sqlite"] = functional_node_dispatcher


if "waste" not in labels.node_types:
    labels.lci_node_types.append("waste")
if "nonfunctional" not in labels.node_types:
    labels.other_node_types.append("nonfunctional")

# make sure allocation happens on parameter changes
def _init_signals():
    from bw2data.signals import on_activity_parameter_recalculate

    on_activity_parameter_recalculate.connect(_check_parameterized_exchange_for_allocation)

def _check_parameterized_exchange_for_allocation(_, name):
    import bw2data as bd
    from bw2data.parameters import ParameterizedExchange
    from bw2data.backends import ExchangeDataset

    databases = [k for k, v in bd.databases.items() if v["backend"] == "functional_sqlite"]

    p_exchanges = ParameterizedExchange.select().where(ParameterizedExchange.group==name)
    exc_ids = [p_exc.exchange for p_exc in p_exchanges]
    exchanges = ExchangeDataset.select(ExchangeDataset.output_database, ExchangeDataset.output_code).where(
        (ExchangeDataset.id.in_(exc_ids)) &
        (ExchangeDataset.type == "production") &
        (ExchangeDataset.output_database.in_(databases))
    )
    process_keys = set(exchanges.tuples())

    for key in process_keys:
        process = bd.get_activity(key)
        if not isinstance(process, Process):
            log.warning(f"Process {key} is not an instance of Process, skipping allocation check.")
            continue
        process.allocate()

_init_signals()
