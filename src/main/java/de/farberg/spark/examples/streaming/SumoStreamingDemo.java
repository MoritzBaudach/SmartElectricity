package de.farberg.spark.examples.streaming;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.uniluebeck.itm.util.logging.Logging;
import scala.Tuple2;

public class SumoStreamingDemo {
	private static final String host = "localhost";

	public static void main(String[] args) throws Exception {
		Logging.setLoggingDefaults();
		Logger log = LoggerFactory.getLogger(SumoStreamingDemo.class);

		String fileName = "src/main/resources/mockData.json";

		JSONParser parser = new JSONParser();
		try {

			//Prepare all data for streaming
			Object obj = parser.parse(new FileReader(fileName));
			JSONArray jsonArray = (JSONArray) obj;
			//JSONObject jsonObject = (JSONObject)obj;

			ArrayList<HouseHold> houseHolds = new ArrayList<>();

			for (Object aObject : jsonArray) {

				HouseHold tempHousehold = new HouseHold();
				tempHousehold.deviceMessages = new ArrayList<>();

				JSONObject jsonObject = (JSONObject) aObject;    //one household
				String householdID = (String) jsonObject.get("household_id"); //save household id
				String regionID = (String) jsonObject.get("region_id").toString(); //save region id
				JSONArray devices = (JSONArray) jsonObject.get("devices");//device list of one household

				int solarpanelMax = ((Long)jsonObject.get("solarpanelMax")).intValue(); //max production of a solarpanel at best conditions
				JSONArray solarProductionArray = (JSONArray) jsonObject.get("solarpanelProduction");

				//get data from devices
				ArrayList<String> deviceMessages = new ArrayList<>();
				for (Object aDevice : devices) {
					JSONObject mDevice = (JSONObject) aDevice; //one device
					String deviceID = (String) mDevice.get("id"); //save device id
					String duration = (String) mDevice.get("durationMinutes").toString(); //save duration
					Boolean readyState = (Boolean) mDevice.get("stoppable");
					String consumptionPerUsage = (String) mDevice.get("consumptionPerUsage").toString();

					String currentDeviceMessage = "regionID=" + regionID + ",householdID=" + householdID + ",deviceID=" + deviceID + ",readystate=" + readyState.toString() + ",consumption=" + consumptionPerUsage + ",duration=" + duration;
					deviceMessages.add(currentDeviceMessage);
				}

				//get data from solarpanels
				ArrayList<String> solarMessages = new ArrayList<>();
				for(Object temp : solarProductionArray){
					JSONObject solarDataPoint = (JSONObject) temp;
					int solarDatasetCounter = ((Long)solarDataPoint.get("counter")).intValue();
					String timeStamp = (String) solarDataPoint.get("timestamp");
					double watts = Double.parseDouble(solarDataPoint.get("watt").toString());
					String currentSolarMessage = "regionID=" + regionID + ",householdID=" + householdID + ",maxoutput="+solarpanelMax+",counter="+solarDatasetCounter+",timestamp="+timeStamp+",watt:"+watts;
					solarMessages.add(currentSolarMessage);
				}

				// TODO: 27.07.2016 If we have time we should change to an intelligent way for mixing both datasets
				//mix solar and device data
				tempHousehold.deviceMessages.addAll(deviceMessages);
				//tempHousehold.deviceMessages.addAll(solarMessages);


				houseHolds.add(tempHousehold);
			}


			//create threads for each household that are sending the data

			//this is for creating multiple sending threads
			/*
			for(HouseHold houseHold : houseHolds) {

				Iterator iterator = houseHold.deviceMessages.iterator();

				ServerSocketSource<String> serverSocketSource = new ServerSocketSource<String>(() -> {

					//return null if there is no more data available
					if (!iterator.hasNext()) {
						log.info("Streaming of mock data for this household finished.");
						return null;
					}

					// Read the next line
					return (String) iterator.next();
				}, () -> 100);
			}*/


			//single thread sending

			//combine all messages into one arraylist
			ArrayList<String> messages = new ArrayList<>();
			for (HouseHold tempHouseHold : houseHolds) {
				messages.addAll(tempHouseHold.deviceMessages);
			}

			Iterator iterator = messages.iterator();
			//create sending object
			ServerSocketSource dataSource = new ServerSocketSource(()->{

				//return null if there is no more data available
				if (!iterator.hasNext()) {
					log.info("Streaming finished!");
					return null;
				}

				// Read the next line
				return (String) iterator.next();

			},()->100);



		//get data

		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local[2]");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		//assure that port is already set before listening to the port
		while(!dataSource.isRunning()){
			Thread.sleep(1000);
		}

		// Create a JavaReceiverInputDStream on target ip:port and count the words in input stream of \n delimited text
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(host, dataSource.getLocalPort(), StorageLevels.MEMORY_AND_DISK_SER);

		//do perform
		//prepare key value pairs

		//differentiate between device and solarpanel data
		//JavaPairDStream<String, String>   lines.filter((Function<String, Boolean>) s -> s.contains("maxoutput"));

		//show line
		lines.print();

		//"regionID=" + regionID + ",householdID=" + householdID + ",deviceID=" + deviceID + ",readystate=" + readyState.toString() + ",consumption=" + consumptionPerUsage + "duration=" + duration
		@SuppressWarnings("resource")
		JavaPairDStream<String, Map<String, String>> mappedLines = lines.mapToPair(line -> {
			Map<String, String> keyValueMap = Arrays.stream(line.split(","))
					.collect(Collectors.toMap(entry -> entry.split("=")[0], entry -> entry.split("=")[1]));

			return new Tuple2<>(keyValueMap.get("householdID"), keyValueMap);
		});

		//we are only interested in devices that are switchable
			JavaPairDStream<String, Map<String, String>> devices = mappedLines.filter(new Function<Tuple2<String, Map<String, String>>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, Map<String, String>> stringMapTuple2) throws Exception {
				return stringMapTuple2._2.get("readystate").equals("true");
			}
		});

		//create key value pairs for the devices
		JavaPairDStream<String, String> device= devices.mapToPair(new PairFunction<Tuple2<String,Map<String,String>>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String, Map<String, String>> stringMapTuple2) throws Exception {
				Map<String,String> localMap = stringMapTuple2._2;
				String householdId = localMap.get("householdID");
				String deviceId = localMap.get("deviceID");
				String consumptionPerUsage = localMap.get("consumption");

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

			//latestDevice.print();


		//JavaDStream<String> words = lines.flatMap(x -> Lists.newArrayList(x.split(" ")));

		//JavaPairDStream<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1)).reduceByKey((i1, i2) -> i1 + i2);


		//clean up
		latestDevice.print();
		ssc.start();

		ssc.awaitTermination();
		ssc.close();
		dataSource.stop();


	}catch (Exception e) {
			e.printStackTrace();
		}
	}
}


class HouseHold{
	ArrayList<String> deviceMessages;
}
