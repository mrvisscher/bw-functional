from functools import partial
from typing import Callable, List
from logging import getLogger

from .node_classes import Process, Function

log = getLogger(__name__)


def remove_output(d: dict) -> dict:
    if "output" in d:
        del d["output"]
    return d


def generic_allocation(
    process: Process,
    getter: lambda function: float(),
) -> None:
    """
    Perform allocation for a multifunctional process using a specified getter function.

    This function calculates allocation factors for each function in a multifunctional process
    based on the values returned by the `getter` function. The allocation factor for each function
    is determined by dividing the value returned by the `getter` function for that function by the
    sum of all values returned by the `getter` function for all functions in the process.

    Args:
        process (Process): The process object to allocate. Must be an instance of the `Process` class.
        getter (Callable): A function that takes a `Function` object and returns a float value
            used for allocation calculations.

    Raises:
        ValueError: If the `process` is not an instance of the `Process` class.
        ZeroDivisionError: If the sum of allocation factors is zero.

    Notes:
        - Functions with a positive `substitution_factor` and a non-zero `allocation_factor` will
          have their `allocation_factor` reset to 0.0 and will not be included in the allocation.
        - Functions with a non-positive `substitution_factor` are included in the allocation.
    """
    # Ensure the process is a valid Process instance
    if not isinstance(process, Process):
        raise ValueError("Activity must be a Process instance")

    # Return none if the process is not multifunctional
    if not process.multifunctional:
        return

    # Collect functions eligible for allocation (functions that are not subsituted)
    functions = []
    for fn in process.functions():
        if fn.get("substitution_factor", 0) > 0 and fn["allocation_factor"] > 0:
            # Reset allocation factor for functions with positive substitution factor
            fn["allocation_factor"] = 0.0
            fn.save()
        elif fn.get("substitution_factor", 0) <= 0:
            # Include functions with non-positive substitution factor
            functions.append(fn)

    # Calculate the total value for allocation
    total = sum([getter(function) for function in functions])

    # Raise an error if the total is zero to avoid division by zero
    if not total:
        raise ZeroDivisionError("Sum of allocation factors is zero")

    # Calculate and assign allocation factors for each function
    for i, function in enumerate(functions):
        factor = getter(function) / total
        function["allocation_factor"] = factor
        function.save()


def get_property_value(
    function: Function,
    property_label: str,
) -> float:
    """
    Retrieve the value of a specified property from a given function.

    This function extracts the value of a property identified by `property_label` from the
    `properties` dictionary of the provided `Function` object. If the property is marked
    as normalized, the value is calculated by multiplying the `amount` in the processing
    edge with the `amount` of the property.

    Args:
        function (Function): The function object from which the property value is retrieved.
            Must be an instance of the `Function` class.
        property_label (str): The label of the property to retrieve.

    Returns:
        float: The value of the specified property.

    Raises:
        ValueError: If the provided `function` is not an instance of the `Function` class.
        KeyError: If the `properties` dictionary is missing or the specified property
            is not found in the `properties` dictionary.

    Notes:
        - If the property is stored as a float (legacy format), a warning is logged.
        - If the property is normalized, the value is calculated as:
          `function.processing_edge["amount"] * prop["amount"]`.
    """
    if not isinstance(function, Function):
        raise ValueError("Passed non-function for allocation")

    props = function.get("properties")

    if not props:
        raise KeyError(f"Function {function} from process {function.processor} doesn't have properties")

    prop = props.get(property_label)

    if not prop:
        raise KeyError(f"Function {function} from {function.processor} missing property {property_label}")

    if isinstance(prop, float):
        log.warning("Property using legacy float format")
        return prop

    if prop.get("normalize", False):
        return abs(function.processing_edge["amount"]) * prop["amount"]
    else:
        return prop["amount"]


def property_allocation(property_label: str) -> Callable:
    """
    Create a partial function for allocating a process based on a specific property.

    This function generates a callable that performs allocation for a multifunctional process
    using a getter function that retrieves the value of the specified property.

    Args:
        property_label (str): The label of the property to be used for allocation.

    Returns:
        Callable: A partial function that performs allocation using the specified property.
    """
    getter = partial(get_property_value, property_label=property_label)
    return partial(generic_allocation, getter=getter)


allocation_strategies = {
    "equal": partial(generic_allocation, getter=lambda x: 1.0),
    "manual": partial(generic_allocation, getter=lambda x: x.get("allocation_factor", 1)),
}
