import bw2data as bd


def test_allocation_labels_applied_price(basic):
    n = bd.get_node(code="1")
    assert "mf_strategy_label" not in n

    basic.metadata["default_allocation"] = "price"
    basic.process()

    n = bd.get_node(code="1")

    for function in n.products():
        if function["name"] == "first product - 1":
            assert function["mf_allocation_factor"] == 0.28
        elif function["name"] == "second product - 2":
            assert function["mf_allocation_factor"] == 0.72


def test_allocation_labels_applied_manual(basic):
    n = bd.get_node(code="1")
    assert "mf_strategy_label" not in n

    basic.metadata["default_allocation"] = "manual_allocation"
    basic.process()

    n = bd.get_node(code="1")

    for function in n.products():
        if function["name"] == "first product - 1":
            assert function["mf_allocation_factor"] == 0.2
        elif function["name"] == "second product - 2":
            assert function["mf_allocation_factor"] == 0.8


def test_allocation_labels_applied_equal(basic):
    n = bd.get_node(code="1")
    assert "mf_strategy_label" not in n

    basic.metadata["default_allocation"] = "equal"
    basic.process()

    n = bd.get_node(code="1")

    for function in n.products():
        if function["name"] == "first product - 1":
            assert function["mf_allocation_factor"] == 0.5
        elif function["name"] == "second product - 2":
            assert function["mf_allocation_factor"] == 0.5
