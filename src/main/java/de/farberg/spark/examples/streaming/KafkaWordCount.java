package de.farberg.spark.examples.streaming;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class KafkaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {
		// Parse command line arguments
		if (args.length < 2) {
			System.err.println("Parameters: <zookeepers> <topics>\n" + "  <zookeepers> is list of one or more Kafka brokers\n"
					+ "  <topics> is a list of one or more kafka topics to consume from (comma separated)\n\n");
			System.exit(1);
		}

		// Create context with a 2 seconds batch interval
		SparkConf sparkConf = new SparkConf().setAppName("KafkaWorkCountDemo").setMaster("local[2]");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

		// Create Kafka stream
		String zookeepers = args[0];
		String groupId = "Bla";
		Set<String> topicsSet = new HashSet<>(Arrays.asList(args[1].split(",")));
		Map<String, Integer> topicMap = topicsSet.stream().collect(Collectors.toMap(key -> key, value -> 1));

		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zookeepers, groupId, topicMap);

		// // Get the lines, split them into words, count the words and print
		JavaDStream<String> lines = messages.map((Tuple2<String, String> tuple) -> tuple._2);
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)));
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1))
				.reduceByKey((i1, i2) -> i1 + i2);

		wordCounts.print();

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}
}
