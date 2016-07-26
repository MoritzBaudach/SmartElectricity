package de.farberg.spark.examples.streaming;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.uniluebeck.itm.util.logging.Logging;
import scala.Tuple2;

public class SumoStreamingDemo {
	private static final String host = "localhost";

	public static final String[] CSV_HEADERS_SUMO_TRACI_OUT = { "timestamp", "vehicle-id", "vehicle-type", "location-lat", "location-long",
			"edge-id", "edge-name", "speed", "fuel-consumption", "co2-emission", "co-emission", "hc-emission", "noise-emission",
			"nox-emission", "pmx-emission" };

	public static void main(String[] args) throws IOException {
		Logging.setLoggingDefaults();
		Logger log = LoggerFactory.getLogger(SumoStreamingDemo.class);

		String fileName = "src/main/resources/sumo-sim-out.csv";

		// Set up the parsing of the CSV file
		log.info("Reading file {}", fileName);
		Iterator<CSVRecord> csvIterator = CSVFormat.EXCEL.withHeader(CSV_HEADERS_SUMO_TRACI_OUT)
				.withSkipHeaderRecord()
				.withDelimiter(',')
				.withQuote('"')
				.parse(new FileReader(fileName))
				.iterator();

		// Create a server socket that streams the contents of the file
		AtomicInteger lastTimeStamp = new AtomicInteger(-1);
		ServerSocketSource<String> dataSource = new ServerSocketSource<>(() -> {
			// If no more lines are there, return null
			if (!csvIterator.hasNext()) {
				log.info("CSV file has ended.");
				return null;
			}

			// Read the next line
			CSVRecord record = csvIterator.next();

			// Sleep for some time if the time stamp in the file changes
			int timeStamp = Integer.parseInt(record.get("timestamp"));
			if (timeStamp > lastTimeStamp.get()) {
				log.debug("Skipping to new timestamp {} (was {})", timeStamp, lastTimeStamp);
				lastTimeStamp.set(timeStamp);

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					log.warn("" + e, e);
				}
			}

			// Return the data set as fieldName=value,fieldName=value,... (e.g., vehicle-id=1,co2-emission=123.456,...)
			return Arrays.stream(CSV_HEADERS_SUMO_TRACI_OUT)
					.map(fieldName -> fieldName + "=" + record.get(fieldName))
					.collect(Collectors.joining(","));
		});

		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local[2]");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		// Create a JavaReceiverInputDStream on target ip:port and count the words in input stream of \n delimited text
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(host, dataSource.getLocalPort(), StorageLevels.MEMORY_AND_DISK_SER);

		// Map entries to <vehicle-id, Map<key, value> >-pairs (e.g., (1, < (co2-emission, 123.456), ...))
		@SuppressWarnings("resource")
		JavaPairDStream<String, Map<String, String>> mappedLines = lines.mapToPair(line -> {
			Map<String, String> keyValueMap = Arrays.stream(line.split(","))
					.collect(Collectors.toMap(entry -> entry.split("=")[0], entry -> entry.split("=")[1]));

			return new Tuple2<>(keyValueMap.get("vehicle-id"), keyValueMap);
		});

		// Map entries to <vehicle-id, co2-emission>, e.g., (1, 123.456), ...
		JavaPairDStream<String, Double> co2Map = mappedLines
				.mapToPair(e -> new Tuple2<>(e._1, Double.parseDouble(e._2.get("co2-emission"))));

		// Only take the last entry per car in this time-frame
		JavaPairDStream<String, Double> co2PerCar = co2Map.reduceByKey((a, b) -> b);

		// Compute the CO2 sum during this batch interval
		JavaDStream<Double> co2sum = co2PerCar.map(entry -> entry._2).reduce((a, b) -> a + b);

		// Print the accumulated CO2 emissions for all cars
		co2sum.print();

		// Start the computation
		ssc.start();

		ssc.awaitTermination();
		ssc.close();
		dataSource.stop();
	}

}
