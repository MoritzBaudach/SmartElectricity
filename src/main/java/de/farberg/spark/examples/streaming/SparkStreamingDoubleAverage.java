package de.farberg.spark.examples.streaming;

import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkStreamingDoubleAverage {
	private static final String host = "localhost";

	public static void main(String[] args) {

		// Listen on a server socket and on connection send random values to the socket
		Random r = new Random();
		ServerSocketSource<Double> dataSource = new ServerSocketSource<>(() -> r.nextDouble(), () -> 100);

		// Create the context with batch size window
		SparkConf sparkConf = new SparkConf().setAppName("JavaStreamingDoubleAverage").setMaster("local[2]");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));

		// Create a JavaReceiverInputDStream on target ip:port and count the words in input stream of \n delimited text
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(host, dataSource.getLocalPort(), StorageLevels.MEMORY_AND_DISK_SER);

		JavaDStream<Double> numbers = lines.map(line -> Double.parseDouble(line));
		JavaDStream<Tuple2<Double, Integer>> numbersAndCount = numbers.map(x -> new Tuple2<Double, Integer>(x, 1));
		JavaDStream<Tuple2<Double, Integer>> sumationResult = numbersAndCount
				.reduce((tuple1, tuple2) -> new Tuple2<Double, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));

		JavaDStream<Double> result = sumationResult.map(x -> x._1 / x._2);

		result.print();
		ssc.start();

		ssc.awaitTermination();
		ssc.close();
		dataSource.stop();
	}

}
