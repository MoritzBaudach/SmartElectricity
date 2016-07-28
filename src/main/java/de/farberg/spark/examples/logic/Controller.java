package de.farberg.spark.examples.logic;

import de.farberg.spark.examples.exceptions.AlreadyInDatasetException;

import java.util.ArrayList;
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

    public Controller(){
        data=new ConcurrentHashMap();
        households = new ArrayList<>();
    }

    //singleton for doing operations
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

    public void increaseUpdateCounter(){
        this.updateCounter++;

        if(updateCounter>=9){
            decodeHashMap();
            this.data.clear();
        }

        if(updateCounter>=20){
            turnOnConsumer();
            System.out.println(requestDevicesAndConsumption(0));
        }
    }

    public int getUpdateCounter(){
        return this.updateCounter;
    }

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

    public void turnOnConsumer(){
        for(Household household : this.households){

            //null is the case if we haven´t received solardata so far
            if(household.getProduction()!=null) {
                double availableEnergy = household.getProduction();

                for(Device device : household.getDevicesInHousehold()){
                    if(!device.isOn()){
                        if((availableEnergy-device.getEnergyConsumption())>=0){
                            availableEnergy -=device.getEnergyConsumption();
                            device.switchState();
                        }}
                }
            }
        }
    }


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
                result+="Device "+deviceCounter +"\t"+device.getEnergyConsumption()+"\t"+device.isOn()+"\n";
                deviceCounter++;
            }
        }


        return result;
    }
}
