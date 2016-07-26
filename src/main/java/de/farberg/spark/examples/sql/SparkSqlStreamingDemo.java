package de.farberg.spark.examples.sql;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SparkSqlStreamingDemo {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("egal").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		List<ElectricConsumer> data = Arrays.asList(
				new ElectricConsumer(10, 49, 2, true), 
				new ElectricConsumer(11, 47, 3, false));
		
		JavaRDD<ElectricConsumer> distData = sc.parallelize(data);

		DataFrame dataFrame = sqlContext.createDataFrame(distData, ElectricConsumer.class);
		dataFrame.registerTempTable("consumers");

		DataFrame activeConsumersDataFrame = sqlContext
				.sql("SELECT posLat, posLon, kwH, switchedOn FROM consumers WHERE switchedOn = true");

		List<String> activeConsumers = activeConsumersDataFrame.javaRDD()
				.map(row -> "posLon: " + row.getDouble(1) + ", postLat: " + row.getDouble(0) + ", kwh: " + row.getInt(2))
				.collect();

		System.out.println("------------------------------------------------------------");
		for (String consumer : activeConsumers)
			System.out.println(consumer);
		System.out.println("------------------------------------------------------------");
	}

}
