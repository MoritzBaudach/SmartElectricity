package de.farberg.spark.examples.streaming;

import java.io.FileReader;
import java.util.*;
import java.util.stream.Collectors;

import de.farberg.spark.examples.Webserver.Webserver;
import de.farberg.spark.examples.logic.Controller;
import de.farberg.spark.examples.logic.Household;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
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

public class ElectricityStreamingDemo {
	private static final String host = "localhost";
	private static ArrayList<String> allMessages = new ArrayList<>();

	public static void main(String[] args) throws Exception {
		Logging.setLoggingDefaults();
		Logger log = LoggerFactory.getLogger(ElectricityStreamingDemo.class);

		String fileName = "src/main/resources/mockData.json";

		JSONParser parser = new JSONParser();
		try {
			System.out.println("Start with preparation of mock data");

			//Prepare all data for streaming
			Object obj = parser.parse(new FileReader(fileName));
			JSONArray jsonArray = (JSONArray) obj;
			//JSONObject jsonObject = (JSONObject)obj;

			for (Object aObject : jsonArray) {

				JSONObject jsonObject = (JSONObject) aObject;    //one household
				String householdID = (String) jsonObject.get("household_id"); //save household id
				String regionID = (String) jsonObject.get("region_id").toString(); //save region id
				JSONArray devices = (JSONArray) jsonObject.get("devices");//device list of one household


				int solarpanelMax = ((Long)jsonObject.get("solarpanelMax")).intValue(); //max production of a solarpanel at best conditions
				JSONArray solarProductionArray = (JSONArray) jsonObject.get("solarpanelProduction");

				//get data from devices
				ArrayList<ArrayList<String>> allDeviceMessages = new ArrayList<>();
				for (Object aDevice : devices) {
					ArrayList<String> messagesOfOneDevice = new ArrayList<>();
					JSONObject mDevice = (JSONObject) aDevice; //one device
					String deviceID = (String) mDevice.get("id"); //save device id
					String duration = (String) mDevice.get("durationMinutes").toString(); //save duration
					String consumptionPerUsage = (String) mDevice.get("consumptionPerUsage").toString();
                    JSONArray deviceStati = (JSONArray) mDevice.get("status");

                    for(Object aStatus : deviceStati){
                        JSONObject aStatusJson = (JSONObject)aStatus;
                       Boolean readyState = (Boolean) aStatusJson.get("readyState");
                       messagesOfOneDevice.add("regionID=" + regionID + ",householdID=" + householdID + ",deviceID=" + deviceID + ",readystate=" + readyState.toString() + ",consumption=" + consumptionPerUsage + ",duration=" + duration);
                    }

                    //add device array to array of all devices
					allDeviceMessages.add(messagesOfOneDevice);
				}

				//merge messages from all devices -> we can assume that there are the same amount of messages all the time
				ArrayList<String> deviceMessages = new ArrayList<>();
				for(int i = 0; i < (allDeviceMessages.get(0)).size(); i++){
					for(ArrayList<String> reference : allDeviceMessages){
						deviceMessages.add(reference.get(i));
					}
				}

				//get data from solarpanels
				ArrayList<String> solarMessages = new ArrayList<>();
				for(Object temp : solarProductionArray){
					JSONObject solarDataPoint = (JSONObject) temp;
					int solarDatasetCounter = ((Long)solarDataPoint.get("counter")).intValue();
					String timeStamp = (String) solarDataPoint.get("timestamp");
					double watts = Double.parseDouble(solarDataPoint.get("watt").toString());
					solarMessages.add("regionID=" + regionID + ",householdID=" + householdID + ",maxoutput="+solarpanelMax+",counter="+solarDatasetCounter+",timestamp="+timeStamp+",watt="+watts);
				}


				//mix solar and device data
				//determine which datastructure is bigger
				int deviceCount=0;
				if(deviceMessages.size()>solarMessages.size()){

					//mix both data
					for(int i = 0; i<solarMessages.size();i++){
						int deviceEndCount = deviceCount+allDeviceMessages.size();
						while(deviceCount<deviceEndCount){
							allMessages.add(deviceMessages.get(deviceCount));
							deviceCount++;
						}
						allMessages.add(solarMessages.get(i));
					}

					//add the rest to the array
					allMessages.addAll(deviceMessages.subList(solarMessages.size(),deviceMessages.size()));


				}else{

					//mix both data

					for(int i = 0; i<deviceMessages.size();i++){
						int deviceEndCount = deviceCount+allDeviceMessages.size();
						while(deviceCount<deviceEndCount){
							allMessages.add(deviceMessages.get(deviceCount));
							deviceCount++;
						}
						allMessages.add(solarMessages.get(i));
					}

					//add the rest to the array
					allMessages.addAll(solarMessages.subList(deviceMessages.size(),solarMessages.size()));
				}
			}

			System.out.println("Finished preparation of mock data");


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

			Iterator iterator = allMessages.iterator();
			//create sending object

			ServerSocketSource dataSource = new ServerSocketSource(()->{

				//return null if there is no more data available
				if (!iterator.hasNext()) {
					log.info("Streaming finished!");
					return null;
				}

				// Read the next line
				return (String) iterator.next();

			},()->40);


            //get data

		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local[2]");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(750));


		//assure that port is already set before listening to the port
		while(!dataSource.isRunning()){
			Thread.sleep(1000);
		}



            // Create a JavaReceiverInputDStream on target ip:port and count the words in input stream of \n delimited text
            JavaReceiverInputDStream<String> lines = ssc.socketTextStream(host, dataSource.getLocalPort(), StorageLevels.MEMORY_AND_DISK_SER);


            //show line
            //lines.print();

            //Devices and Solar Panels are streamed to the same socket - therefore we need to filter them beforehead
            JavaDStream<String> mappedDevices = lines.filter((s) -> s.contains("deviceID"));
            JavaDStream<String> mappedSolarPanels = lines.filter((s) -> s.contains("maxoutput"));


			Webserver.start();

            //SOLARPANELS
            //
            //
           JavaPairDStream<String, Map<String, String>> mappedSolarPanelLines = mappedSolarPanels.mapToPair(line -> {
                Map<String, String> keyValueMap = Arrays.stream(line.split(","))
                        .collect(Collectors.toMap(entry -> entry.split("=")[0], entry -> entry.split("=")[1]));
                return new Tuple2<>(keyValueMap.get("householdID"), keyValueMap);
            });


            JavaPairDStream<String, String> solarPanels = mappedSolarPanelLines.mapToPair(stringMapTuple2 -> {
                Map<String, String> localMap = stringMapTuple2._2;
                String householdId = localMap.get("householdID");
                String currentProduction = localMap.get("watt");
                return new Tuple2<String, String>(householdId, currentProduction);
            });

            JavaPairDStream<String, String> latestSolarPanel = solarPanels.reduceByKey((a, b) -> b);


            //DEVICES
            //
            //
            // "regionID=" + regionID + ",householdID=" + householdID + ",deviceID=" + deviceID + ",readystate=" + readyState.toString() + ",consumption=" + consumptionPerUsage + "duration=" + duration
            @SuppressWarnings("resource")
            JavaPairDStream<String, Map<String, String>> mappedDeviceLines = mappedDevices.mapToPair(line -> {
                Map<String, String> keyValueMap = Arrays.stream(line.split(","))
                        .collect(Collectors.toMap(entry -> entry.split("=")[0], entry -> entry.split("=")[1]));

                return new Tuple2<>(keyValueMap.get("deviceId"), keyValueMap);
            });


            //we are only interested in devices that are switchable
            JavaPairDStream<String, Map<String, String>> devices = mappedDeviceLines.filter(new Function<Tuple2<String, Map<String, String>>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, Map<String, String>> stringMapTuple2) throws Exception {
                    return stringMapTuple2._2.get("readystate").equals("true");
                }
            });

            //create key value pairs for the devices
            JavaPairDStream<String, String> device = devices.mapToPair(new PairFunction<Tuple2<String, Map<String, String>>, String, String>() {
                @Override
                public Tuple2<String, String> call(Tuple2<String, Map<String, String>> stringMapTuple2) throws Exception {
                    Map<String, String> localMap = stringMapTuple2._2;
                    String householdId = localMap.get("householdID");
                    String deviceId = localMap.get("deviceID");
                    String consumptionPerUsage = localMap.get("consumption");

                    String key = householdId + "_" + deviceId;
                    return new Tuple2<String, String>(key, consumptionPerUsage);

                }
            });

            //If the same device streams several times before the micro batch processing, only the latest device message is used
            JavaPairDStream<String, String> latestDevice = device.reduceByKey((a, b) -> b);

            //Unite both Streams
            JavaPairDStream deviceAndSolarStream = latestDevice.union(latestSolarPanel);

			JavaPairDStream<String, Double> dataStream = deviceAndSolarStream;

			dataStream.print();

			//updateStream.print();

            //updatefunction
            dataStream.foreachRDD((rdd)-> {
               rdd.foreach((k)->{
               	Controller.getInstance().data.put(k._1, k._2);
				Controller.getInstance().increaseUpdateCounter();
			   });
            });

            //clean up
            //deviceAndSolarStream.print();

            ssc.start();

            ssc.awaitTermination();
            ssc.close();
            dataSource.stop();

		System.out.println("Demo finished");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
