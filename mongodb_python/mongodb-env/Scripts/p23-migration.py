from pyspark.sql import SparkSession

# Configurar SparkSession
spark = SparkSession.builder \
    .appName("MySQL to MongoDB Migration") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/db.collection") \
    .getOrCreate()

# Leer datos desde MySQL
mysql_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://mysql_host:3306/curso") \
    .option("dbtable", "ventas") \
    .option("user", "root") \
    .option("password", "5N]S9-Rt") \
    .load()

# Escribir datos en MongoDB
mysql_df.write.format("mongo") \
    .mode("overwrite") \
    .option("spark.mongodb.output.uri", "mongodb://localhost:27017/db.collection") \
    .save()

# Cerrar SparkSession
spark.stop()
