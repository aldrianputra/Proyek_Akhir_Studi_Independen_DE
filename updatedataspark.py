from pyspark.sql import SparkSession

# Parameter koneksi PostgreSQL
jdbc_url = "jdbc:postgresql://hostname:port/database"
connection_properties = {
    "user": "aldrian",
    "password": #pasworf,
    "driver": "org.postgresql.Driver"
}

# Konfigurasi tabel
table_configs = {
    "movies": {
        "hive_table": "dim_movies",
        "unique_key": "movieid"
    },
    "useraccount": {
        "hive_table": "dim_users",
        "unique_key": "userid"
    },
    "historytransaction": {
        "hive_table": "fact_transactions",
        "unique_key": "transactionid"
    }
}

# Fungsi untuk memuat dan mentransformasi data baru
def load_and_transform_new_data(spark, table_name, unique_key_column, hive_table_name):
    source_df = spark.read.jdbc(jdbc_url, table_name, properties=connection_properties)
    existing_keys_df = spark.sql(f"SELECT DISTINCT {unique_key_column} FROM {hive_table_name}")
    new_data_df = source_df.join(existing_keys_df, on=unique_key_column, how="left_anti")

    # Logika transformasi data (sesuaikan dengan tabel yang diupdate)
    if table_name == "movies":
        new_data_df = new_data_df.select("movieid", "title", "genre").distinct()
    elif table_name == "useraccount":
        new_data_df = new_data_df.select("userid", "username", "email").distinct()
    elif table_name == "historytransaction":
        userhistory_df = spark.read.jdbc(jdbc_url, "userhistory", properties=connection_properties)
        new_data_df = userhistory_df.join(new_data_df, userhistory_df["userid"] == new_data_df["userid"], "inner") \
            .select(
                userhistory_df["userid"],
                userhistory_df["title"],
                userhistory_df["genre"],
                new_data_df["movieid"],
                new_data_df["transactionid"],
                new_data_df["loan_date"],
                new_data_df["return_date"],
                new_data_df["payment"]
            )

    return new_data_df

# Fungsi untuk menyimpan ke Hive
def save_to_hive(df, table_name):
    df.write.mode("append").insertInto(table_name)

def main():
    spark = SparkSession.builder.appName("ETLProcess") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://localhost:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    # Proses pembaruan tabel
    for pg_table, config in table_configs.items():
        new_data_df = load_and_transform_new_data(spark, pg_table, config["unique_key"], config["hive_table"])
        if new_data_df and not new_data_df.rdd.isEmpty():
            save_to_hive(new_data_df, config["hive_table"])

    spark.stop()

if __name__ == "__main__":
    main()

