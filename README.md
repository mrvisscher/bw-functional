# bw-functional

[![PyPI](https://img.shields.io/pypi/v/bw-functional.svg)][pypi status]
[![Status](https://img.shields.io/pypi/status/bw-functional.svg)][pypi status]
[![Python Version](https://img.shields.io/pypi/pyversions/bw-functional)][pypi status]
[![License](https://img.shields.io/pypi/l/bw-functional)][license]

[![Read the documentation at https://multifunctional.readthedocs.io/](https://img.shields.io/readthedocs/multifunctional/latest.svg?label=Read%20the%20Docs)][read the docs]
[![Tests](https://github.com/mrvisscher/bw-functional/actions/workflows/python-test.yml/badge.svg)][tests]
[![Codecov](https://codecov.io/gh/brightway-lca/multifunctional/branch/main/graph/badge.svg)][codecov]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]

[pypi status]: https://pypi.org/project/multifunctional/
[read the docs]: https://multifunctional.readthedocs.io/
[tests]: https://github.com/brightway-lca/multifunctional/actions?workflow=Tests
[codecov]: https://app.codecov.io/gh/brightway-lca/multifunctional
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black

Adding functions to Brightway processes

## Installation

You can install _bw-functional_ via [pip] from [PyPI]:

```console
$ pip install bw-functional
```

[//]: # (It is also available on `anaconda` using `mamba` or `conda` at the `cmutel` channel:)

[//]: # ()
[//]: # (```console)

[//]: # (mamba install -c conda-forge -c cmutel multifunctional)

[//]: # (```)

## Usage

Multifunctional activities can lead to linear algebra problems which don't have exactly one solution. Therefore, we commonly need to apply a handling function to either partition such activities, or otherwise manipulate their data such that they allow for the creation of a square and non-singular technosphere matrix.

This library is designed around the following workflow:

Users create and register a `bw_functional.FunctionalSQLiteDatabase`. Registering this database must include the database metadata key `default_allocation`, which refers to an allocation strategy function present in `bw_functional.allocation_strategies`.

```python
import bw_functional
mf_db = bw_functional.FunctionalSQLiteDatabase("emojis FTW")
mf_db.register(default_allocation="price")
```

Multifunctional process(es) are created and written to the `FunctionalSQLiteDatabase`. A multifunctional process is any process with multiple "functions", either outputs (products) and/or input (reducts).

```python
mf_data = {
    ("emojis FTW", "😼"): {
        "type": "product",
        "name": "meow",
        "unit": "kg",
        "properties": {
            "price": 7,
            "mass": 6,
        },
    },
    ("emojis FTW", "🐶"): {
        "type": "product",
        "name": "woof",
        "unit": "kg",
        "properties": {
            "price": 12,
            "mass": 4,
        },
    },
    ("emojis FTW", "1"): {
        "name": "process - 1",
        "location": "somewhere",
        "exchanges": [
            {
                "type": "production",
                "input": ("emojis FTW", "😼"),
                "amount": 4,
            },
            {
                "type": "production",
                "input": ("emojis FTW", "🐶"),
                "amount": 6,
            },
        ],
    }
}
```
LCA calculations can then be done as normal. See `dev/basic_example.ipynb` for a simple example.

### Substitution

_WORK IN PROGRESS_

### Built-in allocation functions

`multifunctional` includes the following built-in property-based allocation functions:

* `price`: Does economic allocation based on the property "price" in each functional edge.
* `mass`: Does economic allocation based on the property "mass" in each functional edge.
* `manual_allocation`: Does allocation based on the property "manual_allocation" in each functional edge. Doesn't normalize by amount of production exchange.
* `equal`: Splits burdens equally among all functional edges.

Property-based allocation assumes that each `Function` node has a `properties` dictionary, and this dictionary has the relevant key with a corresponding numeric value. For example, for `price` allocation, each `Function` needs to have `'properties' = {'price': some_number}`.

### Custom property-based allocation functions

To create new property-based allocation functions, add an entry to `allocation_strategies` using the function `property_allocation`:

```python
import bw_functional as bf
bf.allocation_strategies['<label in function dictionary>'] = bf.property_allocation(property_label='<property string>')
```

Additions to `allocation_strategies` are not persisted, so they need to be added each time you start a new Python interpreter or Jupyter notebook.

### Custom single-factor allocation functions

To create custom allocation functions which apply a single allocation factor to all nonfunctional inputs and outputs, pass a getter function to `bw_functional.allocation.generic_allocation`. This function needs to accept the following input argument:

* function: An instance of `multifunctional.Function`

The getter should return a number.

The custom getter needs to be curried and added to `allocation_strategies`. You can follow this example:

```python
import bw_functional as bf
from functools import partial

def allocation_factor(function: bf.Function) -> float:
   """Nonsensical allocation factor generation"""
   if function.get("unit") == "kg":
      return 1.2
   elif "silly" in function["name"]:
      return 4.2
   else:
      return 7

bf.allocation_strategies['silly'] = partial(
   bf.generic_allocation,
   getter=allocation_factor,
)
```

[//]: # (### Other custom allocation functions)

[//]: # ()
[//]: # (To have complete control over allocation, add your own function to `allocation_strategies`. This function should take an input of *either* `multifunctional.MaybeMultifunctionalProcess` or a plain data dictionary, and return a list of data dictionaries *including the original input process*. These dictionaries can follow the [normal `ProcessWithReferenceProduct` data schema]&#40;https://github.com/brightway-lca/bw_interface_schemas/blob/5fb1d40587aec2a4bb2248505550fc883a91c355/bw_interface_schemas/lci.py#L83&#41;, but the result datasets need to also include the following:)

[//]: # ()
[//]: # (* `mf_parent_key`: Integer database id of the source multifunctional process)

[//]: # (* `type`: One of "readonly_process", "process", or "multifunctional")

[//]: # ()
[//]: # (Furthermore, the code of the allocated processes &#40;`mf_allocated_process_code`&#41; must be written to each functional edge &#40;and that edge saved so this data is persisted&#41;. See the code in `multifunctional.allocation.generic_allocation` for an example.)

## Technical notes

### Process-specific allocation strategies

Individual processes can override the default database allocation by specifying their own `default_allocation`:

```python
import bw2data
node = bw2data.get(database="emojis FTW", code="1")
node["default_allocation"] = "mass"
node.save()
```

## How does it work?

Recent Brightway versions allow users to specify which graph nodes types should be used when building matrices, and which types can be ignored. We create a multifunctional process node with the type `multifunctional`, which will be ignored when creating processed datapackages. However, in our database class `FunctionalSQLiteDatabase` we change the function which creates these processed datapackages to load the multifunctional processes, perform whatever strategy is needed to handle multifunctionality, and then use the results of those handling strategies (e.g. monofunctional processes) in the processed datapackage.

We also tell `MultifunctionalDatabase` to load a new `ReadOnlyProcess` process class instead of the standard `Activity` class when interacting with the database. This new class is read only because the data is generated from the multifunctional process itself - if updates are needed, either that input process or the allocation function should be modified.

## Contributing

Contributions are very welcome.
To learn more, see the [Contributor Guide][Contributor Guide].

## License

Distributed under the terms of the [BSD 3 Clause license][License],
_multifunctional_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue][Issue Tracker] along with a detailed description.


<!-- github-only -->

[command-line reference]: https://multifunctional.readthedocs.io/en/latest/usage.html
[License]: https://github.com/brightway-lca/multifunctional/blob/main/LICENSE
[Contributor Guide]: https://github.com/brightway-lca/multifunctional/blob/main/CONTRIBUTING.md
[Issue Tracker]: https://github.com/brightway-lca/multifunctional/issues


## Building the Documentation

You can build the documentation locally by installing the documentation Conda environment:

```bash
conda env create -f docs/environment.yml
```

activating the environment

```bash
conda activate sphinx_multifunctional
```

and [running the build command](https://www.sphinx-doc.org/en/master/man/sphinx-build.html#sphinx-build):

```bash
sphinx-build docs _build/html --builder=html --jobs=auto --write-all; open _build/html/index.html
```
