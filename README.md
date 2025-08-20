Assume that your iceberg.db and warehouse are both in the new base directory.
Then you run the following to do the database migration
```shell
python iceberg_migration.py --new_base "/path/to/new/base"
```