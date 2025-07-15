import bw2data as bd
import pytest
from bw2data.tests import bw2test

from bw_functional import FunctionalSQLiteDatabase
from bw_functional.allocation import generic_allocation
from bw_functional.node_classes import Process, Product


def check_basic_allocation_results(factor_1, factor_2, database):
    nodes = sorted(database, key=lambda x: (x["name"], x.get("reference product", "")))
    functions = list(filter(lambda x: isinstance(x, Product), nodes))
    allocated = list(filter(lambda x: isinstance(x, ReadOnlyProcess), nodes))

    # === Checking allocated process 1 ===
    # == Process values ==
    expected = {
        "name": "process - 1 ~ product - 1",
        "code": "2-allocated",
        "full_process_key": nodes[1].key,
        "type": "readonly_process",
    }
    for key, value in expected.items():
        assert allocated[0][key] == value

    # == Production exchange ==
    expected = {
        "input": functions[0].key,
        "output": allocated[0].key,
        "amount": 4,
        "type": "production",
    }
    production = list(allocated[0].production())
    assert len(production) == 1
    for key, value in expected.items():
        assert production[0][key] == value

    # == Biosphere exchange ==
    expected = {
        "input": nodes[0].key,
        "output": allocated[0].key,
        "amount": factor_1,
        "type": "biosphere",
    }
    biosphere = list(allocated[0].biosphere())
    assert len(biosphere) == 1
    for key, value in expected.items():
        assert biosphere[0][key] == value

    assert not biosphere[0].get("functional")

    # === Checking allocated process 2 ===
    # == Process values ==
    expected = {
        "name": "process - 1 ~ product - 2",
        "code": "3-allocated",
        "full_process_key": nodes[1].key,
        "type": "readonly_process",
    }
    for key, value in expected.items():
        assert allocated[1][key] == value

    expected = {
        "input": functions[1].key,
        "output": allocated[1].key,
        "amount": 6,
        "type": "production",
    }
    production = list(allocated[1].production())
    assert len(production) == 1
    for key, value in expected.items():
        assert production[0][key] == value

    expected = {
        "input": nodes[0].key,
        "output": allocated[1].key,
        "amount": factor_2,
        "type": "biosphere",
    }
    biosphere = list(allocated[1].biosphere())
    assert len(biosphere) == 1
    for key, value in expected.items():
        assert biosphere[0][key] == value

    assert not biosphere[0].get("functional")


def test_basic_database(basic):
    assert len(basic) == 4
    assert len([x for x in basic if isinstance(x, Process)]) == 2
    assert len([x for x in basic if isinstance(x, Product)]) == 2



def test_price_allocation(basic):
    basic.metadata["default_allocation"] = "price"
    bd.get_node(code="1").allocate()

    assert bd.get_node(code="2")["allocation_factor"] == 0.75
    assert bd.get_node(code="3")["allocation_factor"] == 0.25


def test_manual_allocation(basic):
    basic.metadata["default_allocation"] = "manual_allocation"
    bd.get_node(code="1").allocate()

    assert bd.get_node(code="2")["allocation_factor"] == 0.1
    assert bd.get_node(code="3")["allocation_factor"] == 0.9


def test_mass_allocation(basic):
    basic.metadata["default_allocation"] = "mass"
    bd.get_node(code="1").allocate()

    assert bd.get_node(code="2")["allocation_factor"] == 0.25
    assert bd.get_node(code="3")["allocation_factor"] == 0.75


def test_equal_allocation(basic):
    basic.metadata["default_allocation"] = "equal"
    bd.get_node(code="1").allocate()


    assert bd.get_node(code="2")["allocation_factor"] == 0.5
    assert bd.get_node(code="3")["allocation_factor"] == 0.5


def test_allocation_no_label(basic):
    del basic.metadata["default_allocation"]
    with pytest.raises(ValueError, match="Can't find `default_allocation` in input arguments, or process/database metadata."):
        bd.get_node(code="1").allocate()

