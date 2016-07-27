package de.farberg.spark.examples.logic;

import de.farberg.spark.examples.exceptions.AlreadyInDatasetException;

import java.util.ArrayList;

/**
 * Created by krischke on 27.07.2016.
 */
public class Household {

    private String id;
    private String regionId;

    private ArrayList<Device> devicesInHousehold;

    public static ArrayList<String> deviceMessages;

    public Household(String id, String regionId){
        setId(id);
        setRegionId(regionId);

        //initialize Arrays
        devicesInHousehold = new ArrayList<>();
        deviceMessages = new ArrayList<>();
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }


    public String getRegionId() {
        return this.regionId;
    }

    public void setRegionId(String id) {
        this.id = id;
    }



    public void addDevice(Device device) throws AlreadyInDatasetException{
        synchronized(this.devicesInHousehold){
            if(findDevice(device.getId())==null){
                this.devicesInHousehold.add(device);
            }else{
                throw new AlreadyInDatasetException();
            }
        }
    }

    public void removeDevice(Device device){
        synchronized(this.devicesInHousehold){
            try{
                if(findDevice(device.getId())!=null){
                    this.removeDevice(device);
                }}catch(NullPointerException ex){
                System.out.println(ex.getMessage());
            }
        }
    }

    public Device findDevice(String id){
        Device result = null;
        for(Device temp : this.devicesInHousehold){
            if(temp.getId().equals(id)){
                result=temp;
                break;
            }
        }
        return result;
    }
}
