from pyspark.sql import SparkSession

# .config("spark.driver.extraClassPath", "C:\\sw\\mongodb_python\\mongodb-env\\jars\\*") \
# com.mongodb.spark.sql.DefaultSource
# Configurar SparkSession
spark = SparkSession.builder \
    .appName("MySQL to MongoDB Migration") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/db.collection") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.3.2") \
    .config("spark.driver.extraClassPath", "C:\\sw\\mongodb_python\\mongodb-env\\jars\\*") \
    .config("spark.jars", "C:\\sw\\mongodb_python\\mongodb-env\\jars\\mongo-java-driver-3.9.0.jar,C:\\sw\\mongodb_python\\mongodb-env\\jars\\mongo-java-driver-3.9.0.jar,C:\\sw\\mongodb_python\\mongodb-env\\jars\\mysql-connector-j-8.4.0") \
    .getOrCreate()

# Leer datos desde MySQL
mysql_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/curso") \
    .option("dbtable", "venta") \
    .option("user", "root") \
    .option("password", "5N]S9-Rt") \
    .load()

# Escribir datos en MongoDB
mysql_df.write.format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.output.uri", "mongodb://localhost:27017/Practica23.ventas") \
    .save()

# Cerrar SparkSession
spark.stop()
