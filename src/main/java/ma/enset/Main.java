package ma.enset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {

        // =====================================================
        // STEP 1: Create Spark Session
        // =====================================================
        SparkSession spark = SparkSession.builder()
                .appName("Bike Sharing Analysis")
                .master("local[*]")
                .getOrCreate();

        // =====================================================
        // STEP 2: Load CSV file
        // =====================================================
        Dataset<Row> df = spark.read()
                .option("header", true)       // first row = column names
                .option("inferSchema", true)  // detect types automatically
                .csv("bike_sharing.csv");

        // =====================================================
        // STEP 3: Display schema
        // 👉 Shows column names and data types
        // =====================================================
        System.out.println("===== SCHEMA =====");
        df.printSchema();

        // =====================================================
        // STEP 4: Show first rows
        // 👉 Preview the dataset
        // =====================================================
        System.out.println("===== FIRST 5 ROWS =====");
        df.show(5);

        // =====================================================
        // STEP 5: Count total rentals
        // =====================================================
        long count = df.count();
        System.out.println("===== TOTAL RENTALS =====");
        System.out.println("Total rentals: " + count);

        // =====================================================
        // STEP 6: Convert start_time to timestamp
        // 👉 Needed for extracting hour later
        // =====================================================
        Dataset<Row> dfClean = df.withColumn(
                "start_time",
                to_timestamp(col("start_time"))
        );

        System.out.println("===== SCHEMA AFTER CONVERSION =====");
        dfClean.printSchema();

        // =====================================================
        // STEP 7: Basic statistics
        // 👉 count, mean, min, max...
        // =====================================================
        System.out.println("===== STATISTICS =====");
        dfClean.describe().show();

        // =====================================================
        // STEP 8: Create SQL view
        // =====================================================
        dfClean.createOrReplaceTempView("bike_rentals_view");
        System.out.println("===== SQL VIEW CREATED =====");

        // =====================================================
        // STEP 9: Total number of rentals (SQL)
        // =====================================================
        System.out.println("===== TOTAL RENTALS (SQL) =====");

        Dataset<Row> total = spark.sql(
                "SELECT COUNT(*) AS total FROM bike_rentals_view"
        );

        total.show();

        // =====================================================
        // STEP 10: Rentals by hour (🔥 IMPORTANT)
        // 👉 Extract hour from start_time
        // =====================================================
        System.out.println("===== RENTALS BY HOUR =====");

        Dataset<Row> byHour = spark.sql(
                "SELECT hour(start_time) AS hr, COUNT(*) AS total " +
                        "FROM bike_rentals_view " +
                        "GROUP BY hour(start_time) " +
                        "ORDER BY hr"
        );

        byHour.show();

        // =====================================================
        // BONUS 🔥: Peak hour (most rentals)
        // =====================================================
        System.out.println("===== PEAK HOUR =====");

        Dataset<Row> peak = spark.sql(
                "SELECT hour(start_time) AS hr, COUNT(*) AS total " +
                        "FROM bike_rentals_view " +
                        "GROUP BY hour(start_time) " +
                        "ORDER BY total DESC LIMIT 1"
        );

        peak.show();

        // =====================================================
        // STOP Spark
        // =====================================================
        spark.stop();
    }
}