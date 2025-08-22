Assume that your iceberg.db and warehouse are both in the new base directory.
Then you run the following to do the database migration
```shell
python iceberg_migration.py --new_base "/path/to/new/base"
```
To use spark to do the migration, you first need to create a directory that is exactly same as your old one 
and put your iceberg.db and warehouse there
```shell

```
Then you need to install pyspark using:
```shell
pip install pyspark
```
and run the following:
```shell
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hadoop \
  migrate_warehouse.py /your/old/warehouse/ /your/new/warehouse/
```