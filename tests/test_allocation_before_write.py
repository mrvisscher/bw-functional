import bw2data as bd
import pytest
from bw2data.tests import bw2test

import bw_functional as mf
from bw_functional.node_classes import Process, Product, ReadOnlyProcess


@pytest.fixture
@bw2test
def allocate_then_write(basic_data):
    db = mf.FunctionalSQLiteDatabase("basic")
    db.register(default_allocation="price")
    db.write(mf.allocation_before_writing(basic_data, "price"), process=False)
    db.process(allocate=False)
    return db


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


def test_without_allocation(allocate_then_write):
    nodes = sorted(allocate_then_write, key=lambda x: (x["name"], x.get("reference product", "")))
    assert len(nodes) == 4

    assert isinstance(nodes[0], Process)
    assert nodes[0]["name"] == "flow - a"
    assert not list(nodes[0].exchanges())

    assert isinstance(nodes[1], Process)
    assert nodes[1].multifunctional
    assert "reference product" not in nodes[1]
    assert "mf_parent_key" not in nodes[1]
    expected = {
        "name": "process - 1",
        "type": "multifunctional",
    }
    for key, value in expected.items():
        assert nodes[1][key] == value


def test_price_allocation(allocate_then_write):
    allocate_then_write.metadata["default_allocation"] = "price"
    bd.get_node(code="1").allocate()
    check_basic_allocation_results(
        4 * 7 / (4 * 7 + 6 * 12) * 10,
        6 * 12 / (4 * 7 + 6 * 12) * 10,
        allocate_then_write,
    )


def test_mass_allocation(allocate_then_write):
    allocate_then_write.metadata["default_allocation"] = "mass"
    bd.get_node(code="1").allocate()
    check_basic_allocation_results(
        4 * 6 / (4 * 6 + 6 * 4) * 10, 6 * 4 / (4 * 6 + 6 * 4) * 10, allocate_then_write
    )


def test_equal_allocation(allocate_then_write):
    allocate_then_write.metadata["default_allocation"] = "mass"
    bd.get_node(code="1").allocate()
    check_basic_allocation_results(5, 5, allocate_then_write)


def test_allocation_uses_existing(allocate_then_write):
    allocate_then_write.metadata["default_allocation"] = "price"
    bd.get_node(code="1").allocate()
    allocate_then_write.metadata["default_allocation"] = "equal"
    bd.get_node(code="1").allocate()
    check_basic_allocation_results(5, 5, allocate_then_write)


def test_allocation_already_allocated(allocate_then_write):
    allocate_then_write.metadata["default_allocation"] = "price"
    bd.get_node(code="1").allocate()
    node = sorted(allocate_then_write, key=lambda x: (x["name"], x.get("reference product", "")))[2]

    with pytest.raises(ValueError):
        mf.generic_allocation(node, None)


def test_allocation_not_multifunctional(allocate_then_write):
    assert mf.generic_allocation(bd.get_node(code="a"), None) == []
