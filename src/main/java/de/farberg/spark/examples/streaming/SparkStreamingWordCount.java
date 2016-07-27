package de.farberg.spark.examples.streaming;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;

import com.google.common.base.Supplier;
import org.apache.avro.generic.GenericData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.collect.Lists;

import org.codehaus.janino.Java;
import scala.Tuple2;

public class SparkStreamingWordCount {
	private static final String host = "localhost";
	public static ArrayList<String> ar;

	public static void main(String[] args) throws FileNotFoundException {

		ar = new ArrayList<String>();
		ar.add("2349,123123123,789789,true");
		ar.add("2200,123123123,789789,true");

		java.util.function.Supplier<String> s = new java.util.function.Supplier<String>() {
			@Override
			public String get() {
				String r = "";
				Iterator i = ar.iterator();
				while (i.hasNext()){
					r = (String)i.next();
				}

				return r;
			}
		};

		// Create a server socket data source that sends string values every 100mss
		//consumptionPerUsage, deviceId, householdId
		ServerSocketSource dataSource = new ServerSocketSource(s, () -> 100);


		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local[2]");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		// Create a JavaReceiverInputDStream on target ip:port and count the words in input stream of \n delimited text
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(host, dataSource.getLocalPort(), StorageLevels.MEMORY_AND_DISK_SER);

		//Filter for all lines with readyState = true
		JavaDStream<String> readyDevice = lines.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				boolean deviceReadyState = s.contains("true");
				return deviceReadyState;
			}
		});

		//Create key value pairs for devices
		JavaPairDStream<String, String> device = readyDevice.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String deviceData[] = s.split(",");
				String deviceId = deviceData[1];
				String householdId = deviceData[2];
				String consumptionPerUsage = deviceData[0];
				String key = householdId + "_" + deviceId;
				return new Tuple2<String, String>(key, consumptionPerUsage);
			}
		});

		JavaPairDStream<String, String> latestDevice = device.reduceByKey(new Function2<String, String, String>() {
			@Override
			public String call(String s, String s2) throws Exception {
				return s2;
			}
		});




		//JavaDStream<String> words = lines.flatMap(x -> Lists.newArrayList(x.split(" ")));

		//JavaPairDStream<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1)).reduceByKey((i1, i2) -> i1 + i2);

		latestDevice.print();
		ssc.start();

		ssc.awaitTermination();
		ssc.close();
		dataSource.stop();
	}


}
