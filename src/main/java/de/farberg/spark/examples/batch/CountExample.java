package de.farberg.spark.examples.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.uniluebeck.itm.util.logging.Logging;

public class CountExample {

	public static void main(String[] args) {
		Logging.setLoggingDefaults();

		String logFile = "src/main/resources/log4j.properties";
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> logData = sc.textFile(logFile).cache();

		// Transformation
		JavaRDD<String> filteredData = logData.filter(s -> s.contains("log4j"));

		// Action
		long numLog4js = filteredData.count();

		// Transformation + Action in one line of code
		long numBs = logData.filter(s -> s.contains("b")).count();

		sc.close();

		System.out.println("Lines with log4j: " + numLog4js + ", lines with b: " + numBs);
	}
}
