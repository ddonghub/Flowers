print("Hello")


spark = SparkSession.builder.master("local[*]").getOrCreate()

flowers_df = spark.read.csv("iris.data", inferSchema="true", header="false")
flowers_df = flowers_df.toDF(
        "sepal_length",
        "sepal_width",
        "petal_length",
        "petal_width",
        "class"
    )
flowers_df.createOrReplaceTempView("flowers")
flowers_df.persist(StorageLevel.DISK_ONLY)
flowers_df.show()
results_overall_1 = spark.sql(
        """
        SELECT
                class
                , COUNT(*) AS cnt
                , AVG(sepal_length) AS sepal_length_avg
                , AVG(sepal_width) AS sepal_width_avg
                , AVG(petal_length) AS petal_length_avg
                , AVG(petal_width) AS petal_width_avg
            FROM flowers
            GROUP BY class
        """
    )
results_overall_1.show()
results_overall_2 = spark.sql(
        """
        SELECT
                COUNT(*) AS cnt
                , AVG(sepal_length) AS sepal_length_avg
                , AVG(sepal_width) AS sepal_width_avg
                , AVG(petal_length) AS petal_length_avg
                , AVG(petal_width) AS petal_width_avg
            FROM flowers
        """
    )
results_overall_2.show()
