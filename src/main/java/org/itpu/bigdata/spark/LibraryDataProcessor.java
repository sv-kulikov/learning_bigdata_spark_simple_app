package org.itpu.bigdata.spark;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class LibraryDataProcessor {

    // If you have error like " ... java.lang.UnsatisfiedLinkError:
    //  'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)' ... "
    // make sure you followed this advice: https://stackoverflow.com/questions/41851066/exception-in-thread-main-java-lang-unsatisfiedlinkerror-org-apache-hadoop-io
    // In short: you have to set HADOOP_HOME variable, add %HADOOP_HOME%/bin to PATH,
    // download hadoop.dll and winutils.exe and place them into %HADOOP_HOME%/bin directory.
    // Under Win10 it works with this version: https://github.com/kontext-tech/winutils/tree/master/hadoop-3.4.0-win10-x64

    // If you have error like " ... Exception in thread "main" java.lang.IllegalAccessError:
    //  class org.apache.spark.storage.StorageUtils$ (in unnamed module ...) cannot access
    //  class sun.nio.ch.DirectBuffer (in module java.base) because module java.base does
    //  not export sun.nio.ch to unnamed module ... "
    // Follow this advice: https://stackoverflow.com/questions/73465937/apache-spark-3-3-0-breaks-on-java-17-with-cannot-access-class-sun-nio-ch-direct
    // In short: add this JVM Option in IDEA IDE: "--add-exports java.base/sun.nio.ch=ALL-UNNAMED"

    // The "library.csv" file is stored in the "data_input" folder of this project.
    // The output of processed data will be stored in "data_output" folder of this project.

    // Place your "library.csv" file here (or change the path if needed):
    private static String sourceCSVPath = "./data_input/*.csv";

    // The output will be stored here (change the path if needed):
    private static String resultingCSVPath = "./data_output";

    public static void main(String[] args) {

        // Initialize Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("Library Data Processing")
                .master("local[*]")
                .getOrCreate();

        // Step 1: Read the CSV file
        Dataset<Row> libraryData = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(sourceCSVPath);

        // Step 2: Filter out records with null in 'b_quantity'
        Dataset<Row> nonNullData = libraryData.filter(col("b_quantity").isNotNull());

        // Step 3: Filter out books issued before 1990
        Dataset<Row> filteredData = nonNullData.filter(col("b_year").geq(1990));

        // Step 4: Output the result schema and result itself
        filteredData.printSchema();
        filteredData.show();

        // Step 5: Save the result to a new CSV file
        filteredData.write()
                .option("header", "true")
                .mode(SaveMode.Overwrite)
                .csv(resultingCSVPath);

        // Close the session
        spark.stop();
    }
}