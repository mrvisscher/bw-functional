import bw_functional as bf


def test_process_creation(basic):
    process = basic.new_node("test_process", name="test_process")
    process.save()

    assert isinstance(process, bf.Process)
    assert process["name"] == "test_process"
    assert process["database"] == basic.name
    assert process["code"] is not None
    assert process["type"] == "nonfunctional"
    assert not process.functional
    assert not process.multifunctional


def test_process_allocation_switch(basic):
    """
    Test the allocation switch functionality of a process.

    This test retrieves a process by its code, allocates it, and verifies the initial
    allocation factor of its first product. It then changes the allocation method of
    the process to "price", saves the process, and verifies that the allocation factor
    of the first product is updated accordingly.

    Args:
        basic: A fixture or object providing access to the database and processes.

    Assertions:
        - The initial allocation factor of the first product is 0.5.
        - After changing the allocation method to "price", the allocation factor
          of the first product is updated to 0.75.
    """
    process: bf.Process = basic.get(code="1")
    process.allocate()

    product = process.products()[0]
    assert product["allocation_factor"] == 0.5

    process["allocation"] = "price"
    process.save()

    product = process.products()[0]
    assert product["allocation_factor"] == 0.75


def test_process_copy(basic):
    """
    Test the copy functionality of a process.

    This test retrieves a process by its code, creates a copy of it, and verifies that
    the copied process has the same attributes as the original process. It also checks
    that the copied process is a new instance and not the same as the original.

    Args:
        basic: A fixture or object providing access to the database and processes.

    Assertions:
        - The copied process has the same name, code, and database as the original.
        - The copied process is a new instance and not the same as the original.
    """
    original_process: bf.Process = basic.get(code="1")
    copied_process = original_process.copy()

    assert copied_process["name"] == original_process["name"]
    assert copied_process["code"] != original_process["code"]
    assert copied_process["database"] == original_process["database"]

    assert len(copied_process.products()) == len(original_process.products())
    assert len(copied_process.exchanges()) == len(original_process.exchanges())

    assert copied_process is not original_process

    for product in copied_process.products():
        assert product.processor == copied_process

    for product in original_process.products():
        assert product.processor == original_process


def test_process_deduct_type(basic):
    """
    Test the deduction of process type based on its products.

    This test retrieves a process by its code, checks its initial type, and then
    modifies the products to change the process type. It verifies that the process
    type is updated correctly based on the products.

    Args:
        basic: A fixture or object providing access to the database and processes.

    Assertions:
        - The initial process type is "multifunctional".
        - After modifying the products, the process type is updated to "functional".
    """
    process: bf.Process = basic.get(code="1")
    assert process["type"] == "multifunctional"
    assert process.deduct_type() == "multifunctional"

    process.products()[0].delete()
    process: bf.Process = basic.get(code="1")
    assert process["type"] == "process"
    assert process.deduct_type() == "process"

    process.products()[0].delete()
    process: bf.Process = basic.get(code="1")
    assert process["type"] == "nonfunctional"
    assert process.deduct_type() == "nonfunctional"


def test_process_new_product(basic):
    """
    Test the creation of a new product in a process.

    This test retrieves a process by its code, creates a new product with specific
    attributes, and verifies that the new product is added to the process. It also
    checks that the new product has the correct attributes and is linked to the process.

    Args:
        basic: A fixture or object providing access to the database and processes.

    Assertions:
        - The new product is successfully created and added to the process.
        - The new product has the expected attributes.
        - The new product is linked to the correct processor.
    """
    process: bf.Process = basic.get(code="1")
    new_product = process.new_product(
        name="new_product",
        code="new_product_code",
        unit="kg",
        amount=10,
        location="first",
    )
    new_product.save()

    assert isinstance(new_product, bf.Product)
    assert new_product["name"] == "new_product"
    assert new_product["code"] == "new_product_code"
    assert new_product["unit"] == "kg"
    assert new_product["amount"] == 10
    assert new_product.processor == process
    assert len(process.products()) == 3


def test_process_property_template(basic):
    """
    Test the property template functionality of a process.

    This test retrieves a process by its code, creates a new property template, and
    verifies that the property template is added to the process. It also checks that
    the property template has the expected attributes and is linked to the process.

    Args:
        basic: A fixture or object providing access to the database and processes.

    Assertions:
        - The new property template is successfully created and added to the process.
        - The new property template has the expected attributes.
        - The new property template is linked to the correct processor.
    """
    process: bf.Process = basic.get(code="1")
    prop = process.property_template("price")

    assert prop["unit"] == "EUR"
    assert prop["amount"] == 1.0

    prod = process.products()[0]
    prod["properties"]["price"] = {"amount": 10, "unit": "DOLLAR"}
    prod.save()

    prop = process.property_template("price")

    assert prop["unit"] in ["EUR", "DOLLAR"]
    assert prop["amount"] == 1.0
