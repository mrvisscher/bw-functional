import bw2data as bd
import pytest

import bw_functional as mf


def test_read_only_node(basic):
    basic.metadata["default_allocation"] = "mass"
    bd.get_node(code="1").allocate()
    node = sorted(basic, key=lambda x: (x["name"], x.get("reference product", "")))[2]
    assert isinstance(node, mf.ReadOnlyProcess)

    with pytest.raises(NotImplementedError) as info:
        node.copy()
    assert "This node is read only" in info.value.args[0]

    with pytest.raises(NotImplementedError) as info:
        node["foo"] = "bar"
    assert "This node is read only" in info.value.args[0]

    with pytest.raises(NotImplementedError) as info:
        node.new_edge()
    assert "This node is read only" in info.value.args[0]

    with pytest.raises(NotImplementedError) as info:
        node.new_exchange()
    assert "This node is read only" in info.value.args[0]


def test_read_only_exchanges(basic):
    basic.metadata["default_allocation"] = "mass"
    bd.get_node(code="1").allocate()
    node = sorted(basic, key=lambda x: (x["name"], x.get("reference product", "")))[2]
    assert isinstance(node, mf.ReadOnlyProcess)

    for exc in node.exchanges():
        with pytest.raises(NotImplementedError) as info:
            exc.save()
        assert "Read-only exchange" in info.value.args[0]

        with pytest.raises(NotImplementedError) as info:
            exc["foo"] = "bar"
        assert "Read-only exchange" in info.value.args[0]

        # with pytest.raises(NotImplementedError) as info:
        #     exc.input = node
        # assert 'Read-only exchange' in info.value.args[0]

        # with pytest.raises(NotImplementedError) as info:
        #     exc.output = node
        # assert 'Read-only exchange' in info.value.args[0]


def test_read_only_parent(basic):
    basic.metadata["default_allocation"] = "mass"
    parent = bd.get_node(code="1")
    parent.allocate()
    node = bd.get_node(code="2-allocated")
    assert node.parent == parent


def test_need_parent_id(basic):
    basic.metadata["default_allocation"] = "mass"
    parent = bd.get_node(code="1")
    parent.allocate()
    node = bd.get_node(code="2-allocated")
    node._data.pop("full_process_key")
    with pytest.raises(ValueError) as info:
        node.save()
    assert "full_process_key" in info.value.args[0]
