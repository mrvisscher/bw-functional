import bw2data as bd
from bw2data.tests import bw2test

from bw_functional import FunctionalSQLiteDatabase


@bw2test
def test_node_creation():
    db = FunctionalSQLiteDatabase("test database")
    db.register(default_allocation="price")

    node = db.new_node()
    node["name"] = "foo"
    node.save()

    for node in db:
        assert node["name"] == "foo"
        assert node["database"] == "test database"
        assert node["code"]
        assert node["type"] == "nonfunctional"


@bw2test
def test_node_creation_functional():
    db = FunctionalSQLiteDatabase("test database")
    db.register(default_allocation="price")

    node = db.new_node()
    node["name"] = "foo"
    node.save()

    node.new_product(name="bar", code="bar").save()

    node = bd.get_node(key=node.key)  # reload node
    assert node["name"] == "foo"
    assert node["database"] == "test database"
    assert node["code"]
    assert node["type"] == "process"


@bw2test
def test_node_creation_multifunctional():
    db = FunctionalSQLiteDatabase("test database")
    db.register(default_allocation="price")

    node = db.new_node()
    node["name"] = "foo"
    node.save()

    node.new_product(name="bar", code="bar").save()
    node.new_product(name="zas", code="zas").save()

    node = bd.get_node(key=node.key)  # reload node
    assert node["name"] == "foo"
    assert node["database"] == "test database"
    assert node["code"]
    assert node["type"] == "multifunctional"
