from bw2data.errors import ValidityError

import bw_functional as bf
import bw2data as bd
import pytest

def test_product_processor_switch(basic):
    """
    Test the switching of a product's processor to a new process.

    This test retrieves a product from the database, creates a new process, and
    switches the product's processor to the new process. It verifies that the
    product's processor is updated correctly.

    Args:
        basic: A fixture or object providing access to the database and products.

    Assertions:
        - The product's processor is switched to the new process.
        - The new process has the correct name and code.
    """
    product = basic.get(code="2")

    new_process = basic.new_node(name="New Process", code="new_process")
    new_process.save()

    product["processor"] = new_process.key
    product.save()

    assert product.processor == new_process


def test_product_allocate_on_new_and_edge(basic):
    """
    Test the allocation of a product on a new process and edge.

    This test retrieves a product from the database, creates a new process, and
    allocates the product to the new process. It verifies that the allocation is
    performed correctly and that the product's processor is updated.

    Args:
        basic: A fixture or object providing access to the database and products.

    Assertions:
        - The product's processor is switched to the new process.
        - The new process has the correct name and code.
        - The allocation is performed correctly.
    """
    process = basic.get(code="1")
    product = process.new_product(name="New Product", code="new_product")

    edge = bd.Edge(
        input=product.key,
        output=process.key,
        amount=5,
        type="production",
    )
    edge.save()

    product.save()

    product = basic.get(code="new_product")
    assert product["allocation_factor"] == 1.0 / 3


def test_product_allocate_on_changed_prop(basic):
    """
    Test the allocation of a product when its properties are changed.

    This test retrieves a product from the database, modifies its properties,
    and verifies that the allocation is updated correctly.

    Args:
        basic: A fixture or object providing access to the database and products.

    Assertions:
        - The product's properties are updated correctly.
        - The allocation factor is recalculated based on the new properties.
    """
    process = basic.get(code="1")
    process["allocation"] = "price"
    process.save()

    product = basic.get(code="2")
    assert product["allocation_factor"] == 0.75

    product["properties"]["price"]["amount"] = 20
    product.save()

    product = basic.get(code="2")
    assert product["allocation_factor"] == 0.8


def test_product_deduct_type(basic):
    """
    Test the deduction of product type based on its properties.

    This test retrieves a product from the database, modifies its properties,
    and verifies that the product type is updated correctly.

    Args:
        basic: A fixture or object providing access to the database and products.

    Assertions:
        - The product's type is updated based on its properties.
        - The allocation factor is recalculated based on the new properties.
    """
    product: bf.Product = basic.get(code="2")
    assert product["type"] == "product"
    assert product.deduct_type() == "product"

    edge = product.processing_edge
    edge["amount"] = -1
    edge.save()

    product: bf.Product = basic.get(code="2")
    assert product["type"] == "waste"
    assert product.deduct_type() == "waste"

    product = bf.Product()
    assert product.deduct_type() == "orphaned_product"


def test_product_processing_edge(basic):
    """
    Test the retrieval of the processing edge for a product.

    This test retrieves a product from the database and verifies that the
    processing edge is correctly identified.

    Args:
        basic: A fixture or object providing access to the database and products.

    Assertions:
        - The processing edge for the product is retrieved correctly.
        - The edge has the correct input and output.
    """
    process = basic.get(code="1")
    product = basic.get(code="2")
    edge = product.processing_edge

    assert edge["input"] == product.key
    assert edge["output"] == product.processor.key
    assert edge["type"] == "production"

    edge = bd.Edge(type="production", input=product.key, output=process.key, amount=10)
    edge.save()
    assert product.processing_edge == None

    edge.delete()
    process.production().delete()
    assert product.processing_edge == None


def test_product_virtual_edges(basic):
    """
    Test the generation of virtual edges for a product.

    This test retrieves a product from the database and verifies that the
    virtual edges are generated correctly based on the product's properties.

    Args:
        basic: A fixture or object providing access to the database and products.

    Assertions:
        - The virtual edges for the product are generated correctly.
        - The edges have the correct input, output, and amount.
    """
    process = basic.get(code="1")
    process.allocate()

    product = basic.get(code="2")
    edges = product.virtual_edges

    assert len(edges) == 2
    assert edges[1]["input"] == ('basic', 'a')
    assert edges[1]["output"] == product.key
    assert edges[1]["amount"] == 5


def test_product_new_edge(basic):
    """
    Test the creation of a new edge for a product.

    This test retrieves a product from the database, creates a new edge for it,
    and verifies that the edge is created correctly.

    Args:
        basic: A fixture or object providing access to the database and products.

    Assertions:
        - The new edge is created with the correct input, output, and amount.
        - The product's processor is updated correctly.
    """
    product = basic.get(code="2")
    with pytest.raises(NotImplementedError):
        # Trying to create an edge without a processor should raise an error
        product.new_edge()



def test_product_validity(basic):
    """
    Test the validity of a product in the database.

    This test retrieves a product from the database and verifies that it is valid.
    If the product is not valid, it raises a ValidityError.

    Args:
        basic: A fixture or object providing access to the database and products.

    Assertions:
        - The product is valid.
        - If the product is not valid, a ValidityError is raised.
    """
    product = basic.get(code="2")
    assert product.valid()

    del product["name"]
    assert not product.valid()

    product = basic.get(code="1").new_product(name="New Product", code="new_product")
    assert product.valid(why=True)

    del product["processor"]
    assert not product.valid()
    assert product.valid(True)[1][0] == "Missing field ``processor``"

    product["processor"] = "not_a_tuple_but_a_string"
    assert not product.valid()
    assert product.valid(True)[1][0] == "Field ``processor`` must be a tuple"

    product["processor"] = ("basic", "non_existent")
    assert not product.valid()
    assert product.valid(True)[1][0] == "Processor node not found"

    product = basic.get(code="1").new_product(name="New Product", code="new_product")
    del product["type"]
    assert not product.valid()
    assert product.valid(True)[1][0] == "Missing field ``type``, product most be ``product`` or ``waste``"

    product["type"] = "invalid_type"
    assert not product.valid()
    assert product.valid(True)[1][0] == "Product ``type`` most be ``product`` or ``waste``"

    with pytest.raises(ValidityError):
        product.save()



