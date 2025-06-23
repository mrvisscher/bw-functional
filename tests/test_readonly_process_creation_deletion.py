import bw2data as bd
from bw2data.tests import bw2test

from bw_functional import FunctionalSQLiteDatabase
from bw_functional.allocation import generic_allocation
from bw_functional.node_classes import Process, Product


def test_allocation_creates_readonly_nodes(basic):
    assert len(basic) == 4

    basic.metadata["default_allocation"] = "price"
    bd.get_node(code="1").allocate()
    assert len(basic) == 6

    assert sorted([ds["type"] for ds in basic]) == [
        "emission",
        "multifunctional",
        "product",
        "product",
        "readonly_process",
        "readonly_process",
    ]


def test_node_save_skips_allocation(basic):
    assert len(basic) == 4

    basic.metadata["default_allocation"] = "price"
    bd.get_node(code="1").save()
    assert len(basic) == 4

