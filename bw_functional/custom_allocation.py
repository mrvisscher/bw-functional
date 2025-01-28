import logging
from copy import copy
from dataclasses import dataclass
from enum import Enum
from numbers import Number
from typing import List, Optional, Union

from blinker import signal
from bw2data import Database, databases
from bw2data.backends import Exchange, Node
from bw2data.project import ProjectDataset, projects

from . import allocation_strategies
from .allocation import property_allocation
from .node_classes import Function, Process

DEFAULT_ALLOCATIONS = set(allocation_strategies)


class MessageType(Enum):
    NONNUMERIC_FUNCTION_PROPERTY = "Non-numeric function property"
    NONNUMERIC_PROPERTY = "Non-numeric property"
    MISSING_FUNCTION_PROPERTY = "Missing product property"
    MISSING_PROPERTY = "Missing property"
    ALL_VALID = "All properties found and have correct type"


@dataclass
class PropertyMessage:
    level: int  # logging levels WARNING and CRITICAL
    process_id: int  # Can get object with bw2data.get_node(id=process_id)
    function_id: int  # Can get object with bw2data.get_node(id=product_id)
    message_type: MessageType  # Computer-readable error message type
    message: str  # Human-readable error message


def _get_unified_properties(edge: Exchange):
    try:
        properties = copy(edge.input["properties"])
    except KeyError:
        properties = {}
    if "properties" in edge:
        properties.update(edge["properties"])
    return properties


def list_available_properties(database_label: str, target_process: Optional[Process] = None):
    """
    Get a list of all properties in a database, and check their suitability for use.

    `database_label`: String label of an existing database.
    `target_process`: Optional. If provided, property checks are done only for this process.
                      If not provided, checks are done for the whole database.

    Returns a list of tuples like `(label: str, message: MessageType)`. Note that
    `NONNUMERIC_PROPERTY` is worse than `MISSING_PROPERTY` as missing properties can be assumed to
    be zero, but non-numeric ones break everything.
    """
    if database_label not in databases:
        raise ValueError(f"Database `{database_label}` not defined in this project")
    if target_process is not None and target_process.get("database") != database_label:
        raise ValueError(f"Target process must be also in database `{database_label}`")

    results = {}
    all_properties = set()

    for function in filter(lambda x: isinstance(x, Function), Database(database_label)):
        for key in function.get("properties", {}):
            all_properties.add(key)

    for label in all_properties:
        if target_process:
            errors = process_property_errors(target_process, label)
        else:
            errors = database_property_errors(database_label, label)
        if not errors:
            results[label] = MessageType.ALL_VALID
        elif any(err.message_type == MessageType.NONNUMERIC_FUNCTION_PROPERTY for err in errors):
            results[label] = MessageType.NONNUMERIC_PROPERTY
        else:
            results[label] = MessageType.MISSING_PROPERTY

    return results


def process_property_errors(process: Process, property_label: str) -> List[PropertyMessage]:
    """
    Check that the given property is present for all functional edges in a given process.

    `process`: Multifunctional process `Node`.
    `property_label`: String label of the property to be used for allocation.

    If all the needed data is present, returns `True`.

    If there is missing data, returns a list of `PropertyMessage` objects.
    """
    messages = []
    if not isinstance(process, Process):
        raise TypeError("Node should be the Process type")

    for function in process.functions():
        properties = function.get("properties", {})
        if property_label not in properties:
            messages.append(
                PropertyMessage(
                    level=logging.WARNING,
                    process_id=process.id,
                    function_id=function.id,
                    message_type=MessageType.MISSING_FUNCTION_PROPERTY,
                    message=f"""Function is missing a property value for `{property_label}`.
Please define this property for the function:
    {function}
Referenced by multifunctional process:
    {process}

""",
                )
            )
        elif not isinstance(properties[property_label], Number) or isinstance(properties[property_label], bool):
            messages.append(
                PropertyMessage(
                    level=logging.CRITICAL,
                    process_id=process.id,
                    function_id=function.id,
                    message_type=MessageType.NONNUMERIC_FUNCTION_PROPERTY,
                    message=f"""Found non-numeric value `{properties[property_label]}` in property `{property_label}`.
Please redefine this property for the function:
    {function}
Referenced by multifunctional process:
    {process}

""",
                )
            )

    return messages


def database_property_errors(database_label: str, property_label: str) -> List[PropertyMessage]:
    """
    Check that the given property is present for all functions in a functional database.

    `database_label`: String label of an existing database.
    `property_label`: String label of the property to be used for allocation.

    If all the needed data is present, returns `True`.

    If there is missing data, returns a list of `PropertyMessage` objects.
    """
    if database_label not in databases:
        raise ValueError(f"Database `{database_label}` not defined in this project")

    db = Database(database_label)
    messages = []

    for process in filter(lambda x: isinstance(x, Process), db):
        messages.extend(process_property_errors(process, property_label))

    return messages

