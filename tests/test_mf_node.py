import bw_functional as bf
import pytest

def test_mf_node_attributes(basic):
    """
    Test the attributes of a multifunctional node.

    This test retrieves a multifunctional node from the database and verifies its
    attributes such as name, code, location, type, and exchanges.

    Args:
        basic: A fixture or object providing access to the database and nodes.

    Assertions:
        - The node has the expected name, code, location, type, and exchanges.
    """
    mf_node = bf.node_classes.MFActivity()

    assert not mf_node.multifunctional

    assert not list(mf_node.exchanges(bf.MFExchanges))

    assert not list(mf_node.technosphere())
    assert not list (mf_node.biosphere())
    assert not list(mf_node.substitution())

    with pytest.raises(NotImplementedError):
        mf_node.rp_exchange()

    edge = mf_node.new_edge()
    assert isinstance(edge, bf.MFExchange)


