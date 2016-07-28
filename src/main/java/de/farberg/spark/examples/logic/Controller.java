package de.farberg.spark.examples.logic;

import com.univocity.parsers.tsv.TsvParser;
import de.farberg.spark.examples.exceptions.AlreadyInDatasetException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Created by krischke on 27.07.2016.
 */
public class Controller {

    private static Controller instance;
    private static Object lock = new Object();

    private ArrayList<Household> households;
    private int updateCounter;
    public ConcurrentHashMap<String,Object> data;

    //constructor
    public Controller(){
        data=new ConcurrentHashMap();
        households = new ArrayList<>();
    }

    //singleton -> there is just a single instance
    public static Controller getInstance(){
        synchronized(lock){
            if(instance==null){
                instance = new Controller();
            }
            return instance;
        }
    }

    public void addHousehold(Household object) throws AlreadyInDatasetException {
        synchronized(this.households){
            if(this.findHousehold(object.getId())==null){
                this.households.add(object);
            }else{
                //throw new AlreadyInDatasetException();
            }
        }
    }

    public void removeHousehold(Household object){
        synchronized(this.households){
            this.households.remove(object);
        }
    }

    public Household findHousehold(String id){
        Household result = null;
        for(Household temp : this.households){
            if(temp.getId().equals(id)){
                result=temp;
                break;
            }
        }
        return result;
    }

    public ArrayList<Household> getHouseholds(){
        return this.households;
    }

    //method that counts all the events happend
    //adds printing messages to console
    public void increaseUpdateCounter(){
        this.updateCounter++;

        if(updateCounter>=9){
            decodeHashMap();
            this.data.clear();
        }

        if(updateCounter>=20){
            turnOnConsumer();

            //just some printing to the console
            System.out.println("\n\n\n\nDevice 0:");
            System.out.println(requestDevicesAndConsumption(0));
            System.out.println("\n\n");

            System.out.println("Device 1:");
            System.out.println(requestDevicesAndConsumption(1));
            System.out.println("\n\n");

            System.out.println("Device 2:");
            System.out.println(requestDevicesAndConsumption(2));
            System.out.println("\n\n");

            System.out.println("Device 3:");
            System.out.println(requestDevicesAndConsumption(3));

            System.out.println("\n\nEvents computed: "+getUpdateCounter());

        }
    }

    public int getUpdateCounter(){
        return this.updateCounter;
    }

    //update java objects with event data
    public void decodeHashMap(){
        for(Map.Entry<String, Object> entry : this.data.entrySet()){
            //get key and value
            String key = entry.getKey();
            Double value = Double.parseDouble((String)entry.getValue());

            if(!key.contains("_")){
                findHousehold(key).setProduction(value);
            }else {
                String [] subStrings = key.split("_");
                Household household = findHousehold(subStrings[0]);
                household.addDevice(subStrings[1], value);
            }

        }
    }

    //logic for turning on devices -> later in depth
    public void turnOnConsumer(){
        for(Household household : this.households){

            //null is the case if we haven´t received solardata so far
            if(household.getProduction()!=null) {
                double availableEnergy = household.getProduction();

                //subtract all energy needed from currently running devices
                for (Device temp : household.getDevicesInHousehold()){
                    if(temp.isOn()){
                        availableEnergy-=temp.getEnergyConsumption();
                    }
                }

                //logic for turning on a device an condition to terminate isOn state
                for(Device device : household.getDevicesInHousehold()){
                    if(!device.getWasOn()){
                    if(!device.isOn()){
                        if((availableEnergy-device.getEnergyConsumption())>=0){
                            availableEnergy -=device.getEnergyConsumption();
                            device.switchState();
                            device.setEndCount(this.getUpdateCounter()+25);
                        }}else {
                        if (device.getEndCount() == getUpdateCounter()) {
                            device.switchState();
                            device.switchWasOn();
                            device.setEnergyConsumption(0);
                        }
                    }
                    }
                }
            }
        }
    }


/*
    public JSONObject requestDevicesAndConsumption(int index){

        String result ="";

        Household household = null;
        int indexCounter=index;

        //filter for the first household that received values from solarpanel
        for(Household temp : this.households){
            if(temp.getProduction()!=null){
                if(indexCounter==0){
                  household=temp;
                  break;
                }
                indexCounter--;
            }
        }

        Map<String, Object> jsonMap = new HashMap<>();
        if(household!=null){

            jsonMap.put("solarpanel", household.getProduction());

            int deviceCounter =0;
            Map<String, Object> jsonArray = new HashMap<>();

            for(Device device : household.getDevicesInHousehold()){
                jsonArray.put("Device "+deviceCounter, device.getEnergyConsumption());
            }
            jsonMap.put("devices", jsonArray);
        }

        JSONObject jsonObject = new JSONObject();
        jsonObject.putAll(jsonMap);


        return jsonObject;
        }
        */

        //method that returns data to a device specified by an index
        public String requestDevicesAndConsumption(int index){
            String result ="";

            Household household = null;
            int indexCounter=index;

            //filter for the first household that received values from solarpanel
            for(Household temp : this.households){
                if(temp.getProduction()!=null){
                    if(indexCounter==0){
                        household=temp;
                        break;
                    }
                    indexCounter--;
                }
            }


            if(household!=null){
                result = "Solarpanel\t"+household.getProduction()+"\n";

                int deviceCounter =0;
                for(Device device : household.getDevicesInHousehold()){
                    result+="Device"+deviceCounter +"\t"+device.getEnergyConsumption()/*+"\t"+device.isOn()+*/+"\n";
                    deviceCounter++;
                }
            }


            return result;
        }

}
