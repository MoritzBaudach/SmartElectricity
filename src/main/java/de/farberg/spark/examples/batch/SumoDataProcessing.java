package de.farberg.spark.examples.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import de.uniluebeck.itm.util.logging.Logging;
import scala.Tuple2;

public class SumoDataProcessing {

	public static void main(String[] args) {
		Logging.setLoggingDefaults();
		String fileName = "src/main/resources/sumo-sim-out.csv";
		// String fileName = "src/main/resources/test.csv"; //Should return 10 for vehicle id 0

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<Row> sumoDataSet = sqlContext.read()
				.format("com.databricks.spark.csv")
				.option("inferSchema", "true")
				.option("header", "true")
				.load(fileName)
				.javaRDD();

		// Action
		System.out.println("Looking at " + sumoDataSet.count() + " data lines");

		sumoDataSet.mapToPair(row -> {
			int vehicleIdIndex = row.fieldIndex("vehicle-id");
			int co2EmissionIndex = row.fieldIndex("co2-emission");
			return new Tuple2<Integer, Double>(row.getInt(vehicleIdIndex), row.getDouble(co2EmissionIndex));

		}).reduceByKey((a, b) -> a + b).sortByKey().collect().forEach(tuple -> {
			System.out.println("Car #" + tuple._1 + ": " + tuple._2 + " mg of CO2 emission");
		});

		sc.close();

	}
}
