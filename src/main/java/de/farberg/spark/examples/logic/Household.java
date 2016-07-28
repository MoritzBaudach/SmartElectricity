package de.farberg.spark.examples.logic;

import de.farberg.spark.examples.exceptions.AlreadyInDatasetException;

import java.util.ArrayList;

/**
 * Created by krischke on 27.07.2016.
 */
public class Household {

    private String id;
    private Double production;

    private ArrayList<Device> devicesInHousehold;

    public static ArrayList<String> deviceMessages;

    public Household(String id){
        setId(id);

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


    public void addDevice(String id, double consumption) {
        synchronized(this.devicesInHousehold){
            if(findDevice(id)==null){
                this.devicesInHousehold.add(new Device(id,consumption));
            }else{
                //throw new AlreadyInDatasetException();
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

    public ArrayList<Device> getDevicesInHousehold(){
        return this.devicesInHousehold;
    }

    public void setProduction(Double energyAvailable){
        this.production = energyAvailable;
    }

    public Double getProduction(){
        return this.production;
    }

}
