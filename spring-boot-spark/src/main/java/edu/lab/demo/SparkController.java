package edu.lab.demo;

import org.apache.spark.sql.*;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import static org.apache.spark.sql.functions.col;
import java.util.ArrayList;
import java.util.List;

@RestController
public class SparkController {

	@GetMapping("/spark")
	public String spark() throws AnalysisException {
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL basic example")
				.config("spark.master", "local[*]")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();
		List<String> data = new ArrayList<String>();
		data.add("New Jersey");
		data.add("Glasgow");
		data.add("Bengaluru");
		data.add("Mumbai");
		data.add("Gurgaon");

		Dataset<Row> df = spark.createDataset(data, Encoders.STRING()).toDF();
		df.printSchema();
		df.show();

		runSparkExamples(spark);
		return "Greetings from Spring Boot Spark ";
	}

	private static void runSparkExamples(SparkSession spark) throws AnalysisException {
		Dataset<Row> df = spark.read().json("src/main/resources/people.json");
		df.show();

		// Print the schema in a tree format
		df.printSchema();
// root
// |-- age: long (nullable = true)
// |-- name: string (nullable = true)

// Select only the "name" column
		df.select("name").show();
// +-------+
// |   name|
// +-------+
// |Michael|
// |   Andy|
// | Justin|
// +-------+

// Select everybody, but increment the age by 1
		df.select(col("name"), col("age").plus(1)).show();
// +-------+---------+
// |   name|(age + 1)|
// +-------+---------+
// |Michael|     null|
// |   Andy|       31|
// | Justin|       20|
// +-------+---------+

// Select people older than 21
		df.filter(col("age").gt(21)).show();
// +---+----+
// |age|name|
// +---+----+
// | 30|Andy|
// +---+----+

// Count people by age
		df.groupBy("age").count().show();
// +----+-----+
// | age|count|
// +----+-----+
// |  19|    1|
// |null|    1|
// |  30|    1|
// +----+-----+

		// Register the DataFrame as a SQL temporary view
		df.createOrReplaceTempView("people");

		Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
		sqlDF.show();
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+

		// Register the DataFrame as a global temporary view
		df.createGlobalTempView("people");

// Global temporary view is tied to a system preserved database `global_temp`
		spark.sql("SELECT * FROM global_temp.people").show();
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+

// Global temporary view is cross-session
		spark.newSession().sql("SELECT * FROM global_temp.people").show();
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+

	}

}