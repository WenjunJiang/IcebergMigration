from pyiceberg.catalog import load_catalog

def test_absolute():
    catalog = load_catalog(
        "default",
        type="sql",
        uri="sqlite:////Users/jwj/data/iceberg/new/iceberg.db",
        warehouse="file:///Users/jwj/data/iceberg/new/warehouse",
    )
    tbl = catalog.load_table("local.my_table")
    print(tbl.name)

def test_relative():
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(
        "default",
        type="sql",
        uri="sqlite:///./iceberg.db",  # relative sqlite path
        warehouse="file:./warehouse",  # relative warehouse (or just "./warehouse")
    )

    tbl = catalog.load_table("local.my_table")
    print(tbl.name)

if __name__=='__main__':
    pass