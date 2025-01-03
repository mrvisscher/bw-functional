import bw2data as bd

import bw_functional as mf


def test_basic_setup(basic):
    flow = bd.get_node(code="a")
    assert isinstance(basic, mf.FunctionalSQLiteDatabase)
    assert isinstance(flow, mf.Process)

    process = bd.get_node(code="1")
    assert isinstance(process, mf.Process)
    assert process.multifunctional
    assert process["type"] == "multifunctional"

    basic.metadata["default_allocation"] = "mass"
    bd.get_node(code="1").allocate()
    node = sorted(basic, key=lambda x: (x["name"], x.get("reference product", "")))[2]
    assert isinstance(node, mf.ReadOnlyProcess)
