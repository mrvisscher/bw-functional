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
                "price": {"amount": 15, "unit": "EUR"},
                "mass": {"amount": 5, "unit": "kg"},
                "manual_allocation": {"amount": 10, "unit": "undefined", "normalized": False},
            },
        },
    ("basic", "3"): {
            "name": "product - 2",
            "code": "3",
            "location": "first",
            "type": "product",
            "unit": "megajoule",
            "properties": {
                "price": {"amount": 5, "unit": "EUR"},
                "mass": {"amount": 15, "unit": "kg"},
                "manual_allocation": {"amount": 90, "unit": "undefined", "normalized": False},
            },
        },
}
