import bw2data as bd
import pytest
from bw2data.tests import bw2test

from bw_functional import FunctionalSQLiteDatabase
from bw_functional.allocation import generic_allocation
from bw_functional.node_classes import Process, Function, ReadOnlyProcess


def check_basic_allocation_results(factor_1, factor_2, database):
    nodes = sorted(database, key=lambda x: (x["name"], x.get("reference product", "")))
    functions = list(filter(lambda x: isinstance(x, Function), nodes))
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


def test_without_allocation(basic):
    nodes = sorted(basic, key=lambda x: (x["name"], x.get("reference product", "")))
    assert len(nodes) == 4

    assert isinstance(nodes[0], Process)
    assert nodes[0]["name"] == "flow - a"
    assert not list(nodes[0].exchanges())
    assert not nodes[0].multifunctional

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

    assert isinstance(nodes[2], Function)
    assert not nodes[2].multifunctional
    assert nodes[2].processor.key == nodes[1].key
    expected = {
        "name": "product - 1",
        "type": "product",
        "processor": nodes[1].key,
    }
    for key, value in expected.items():
        assert nodes[2][key] == value

    assert isinstance(nodes[3], Function)
    assert not nodes[3].multifunctional
    assert nodes[3].processor.key == nodes[1].key
    expected = {
        "name": "product - 2",
        "type": "product",
        "processor": nodes[1].key,
    }
    for key, value in expected.items():
        assert nodes[3][key] == value


def test_price_allocation(basic):
    basic.metadata["default_allocation"] = "price"
    bd.get_node(code="1").allocate()
    check_basic_allocation_results(
        4 * 7 / (4 * 7 + 6 * 12) * 10, 6 * 12 / (4 * 7 + 6 * 12) * 10, basic
    )


def test_manual_allocation(basic):
    basic.metadata["default_allocation"] = "manual_allocation"
    bd.get_node(code="1").allocate()
    check_basic_allocation_results(0.2 * 10, 0.8 * 10, basic)


def test_mass_allocation(basic):
    basic.metadata["default_allocation"] = "mass"
    bd.get_node(code="1").allocate()
    check_basic_allocation_results(
        4 * 6 / (4 * 6 + 6 * 4) * 10, 6 * 4 / (4 * 6 + 6 * 4) * 10, basic
    )


def test_equal_allocation(basic):
    basic.metadata["default_allocation"] = "equal"
    bd.get_node(code="1").allocate()
    check_basic_allocation_results(5, 5, basic)


def test_allocation_uses_existing(basic):
    basic.metadata["default_allocation"] = "price"
    bd.get_node(code="1").allocate()
    basic.metadata["default_allocation"] = "equal"
    bd.get_node(code="1").allocate()
    check_basic_allocation_results(5, 5, basic)


def test_allocation_already_allocated(basic):
    basic.metadata["default_allocation"] = "price"
    bd.get_node(code="1").allocate()
    node = sorted(basic, key=lambda x: (x["name"], x.get("reference product", "")))[2]

    with pytest.raises(ValueError):
        generic_allocation(node, None)


def test_allocation_not_multifunctional(basic):
    assert generic_allocation(bd.get_node(code="a"), None) == []


@bw2test
def test_allocation_zero_factor_still_gives_process():
    DATA = {
        ("basic", "a"): {
            "name": "flow - a",
            "code": "a",
            "unit": "kg",
            "type": "emission",
            "categories": ("air",),
        },
        ("basic", "1"): {
            "name": "process - 1",
            "code": "1",
            "location": "first",
            "type": "multifunctional",
            "exchanges": [
                {
                    "type": "production",
                    "amount": 4,
                    "input": ("basic", "2")
                },
                {
                    "type": "production",
                    "amount": 6,
                    "input": ("basic", "3")
                },
                {
                    "type": "biosphere",
                    "name": "flow - a",
                    "amount": 10,
                    "input": ("basic", "a"),
                },
            ],
        },
        ("basic", "2"): {
            "name": "product - 1",
            "code": "2",
            "location": "first",
            "type": "product",
            "unit": "kg",
            "properties": {
                "price": 7,
                "mass": 6,
                "manual_allocation": 2,
            },
        },
        ("basic", "3"): {
            "name": "product - 2",
            "code": "3",
            "location": "first",
            "type": "product",
            "unit": "megajoule",
            "properties": {
                "price": 0,
                "mass": 4,
                "manual_allocation": 8,
            },
        },
    }

    db = FunctionalSQLiteDatabase("basic")
    db.register(default_allocation="price")
    db.write(DATA)

    for node in db:
        print(node)
        for exc in node.edges():
            print("\t", exc)

    assert bd.get_node(key=("basic", "3-allocated"))
    assert (bd.get_node(name="flow - a"), 0) in [
        (exc.input, exc["amount"])
        for exc in bd.get_node(key=("basic", "3-allocated")).edges()
    ]

