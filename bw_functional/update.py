
latest = "0b89"

def update(version: str | None) -> str:
    if version is None:
        update_0b89()
        return "0b89"
    return version

def update_0b89():
    import bw2data as bd
    import tqdm
    from .node_classes import Process

    bf_db_names = [name for name, dict in bd.databases.items() if dict.get("backend") == "functional_sqlite"]
    for db_name in bf_db_names:
        database = bd.Database(db_name)

        for process in tqdm.tqdm(database, desc=f"Updating {db_name} to bw_functional 0b89", total=len(database)):
            if not isinstance(process, Process):
                continue

            for product in process.products():
                if not product.get("product"):
                    product["product"] = product["name"]

                product._set_inherited("name", process["name"])
                product._set_inherited("database", process["database"])
                product._set_inherited("location", process["location"])

                product.save()

