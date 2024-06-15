from pyspark.sql import SparkSession

# Inisialisasi SparkSession dengan Hive support
spark = SparkSession.builder \
    .appName("ETLProcess") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Koneksi PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/movieku"
connection_properties = {
    "user": "aldrian",
    "password": #pasword,
    "driver": "org.postgresql.Driver"
}

# Memuat data tabel 'movies' 
# Membaca data 'movies' dari PostgreSQL
movies_df = spark.read.jdbc(jdbc_url, "movies", properties=connection_properties)

#  Membuat tabel 'fact_transaction'
# Memuat tabel lainnya dari PostgreSQL
tables_to_load = ["useraccount", "userhistory", "historytransaction"]
dataframes = {table_name: spark.read.jdbc(jdbc_url, table_name, properties=connection_properties) for table_name in tables_to_load}

# Ubah DF ke variable baru agar lebih mudah dibaca
useraccount_df = dataframes["useraccount"]
userhistory_df = dataframes["userhistory"]
historytransaction_df = dataframes["historytransaction"]

# === Transformasi data ===

# 1. Membuat tabel dimensi `dim_users` (hanya kolom yang diperlukan, data unik)
dim_users = useraccount_df.select("userid", "username", "email").distinct()
# 2. Membuat tabel dimensi `dim_movies` (hanya kolom yang diperlukan, data unik)
dim_movies = movies_df.select("movieid", "title", "genre").distinct()
# 3. Melakukan join untuk membentuk tabel fakta `fact_transaction`
fact_transactions = userhistory_df.join(historytransaction_df, userhistory_df["userid"] == historytransaction_df["userid"], "inner") \
    .select(
        userhistory_df["userid"],
        userhistory_df["title"],
        userhistory_df["genre"],
        historytransaction_df["movieid"],
        historytransaction_df["transactionid"],
        historytransaction_df["loan_date"],
        historytransaction_df["return_date"],
        historytransaction_df["payment"]
    )

# Menyimpan tabel dimensi ke Hive
dim_users.write.mode("overwrite").saveAsTable("dim_users")
dim_movies.write.mode("overwrite").saveAsTable("dim_movies")

# Menyimpan 'movies' dan 'fact_transaction' di HDFS dan Hive
movies_df.write.mode("overwrite").parquet("/user/hive/warehouse/movies")
fact_transactions.write.mode("overwrite").parquet("/user/hive/warehouse/fact_transactions")

# Membuat tabel di Hive jika belum ada
spark.sql("DROP TABLE IF EXISTS movies")
spark.sql("CREATE TABLE movies USING parquet LOCATION '/user/hive/warehouse/movies'")
spark.sql("DROP TABLE IF EXISTS fact_transactions")
spark.sql("CREATE TABLE fact_transactions USING parquet LOCATION '/user/hive/warehouse/fact_transactions'")

# Hentikan SparkSession
spark.stop()
