import importlib.util

# Spark tests need pyspark + Java; they run inside the Spark image (`make spark-test`).
collect_ignore = [] if importlib.util.find_spec("pyspark") else ["spark"]
