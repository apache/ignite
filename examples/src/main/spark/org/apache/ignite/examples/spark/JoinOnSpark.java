package org.apache.ignite.examples.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JoinOnSpark {
    /** Run example. */
    public static void main(String[] args) throws InterruptedException {
        // Spark reading and writing to table
        SparkSession spark = SparkSession
            .builder()
            .appName("SparkForIgnite")
            .master("local[2]")
            .getOrCreate();

        Dataset<Row> jt1 = spark.read()
            .option("delimiter", ";")
            .option("header","true")
            .csv("examples/src/main/resources/ds1.csv");

        Dataset<Row> jt2 = spark.read()
            .option("delimiter", ";")
            .option("header","true")
            .csv("examples/src/main/resources/ds2.csv");

        spark.sqlContext().registerDataFrameAsTable(jt1, "jt1");
        spark.sqlContext().registerDataFrameAsTable(jt2, "jt2");

        Dataset<Row> result = spark.sql("SELECT jt1.id as id1, jt1.val1, jt2.id as id2, jt2.val2 " +
            "FROM jt1 LEFT JOIN jt2 ON jt1.val1 = jt2.val2");

        result.show();
        result.explain(true);

    }
}
