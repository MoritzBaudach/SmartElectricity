package de.farberg.spark.examples.logic;

/**
 * Created by krischke on 27.07.2016.
 */
public class Device {

    private String id;
    private double energyConsumption;
    private boolean isOn;

    public Device(String id, double energyConsumption){
        this.setId(id);
        this.setEnergyConsumption(energyConsumption);
        isOn=false;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getEnergyConsumption() {
        return energyConsumption;
    }

    public void setEnergyConsumption(double energyConsumption) {
        this.energyConsumption = energyConsumption;
    }

    public boolean isOn() {
        return isOn;
    }

    public void switchState() {
        if(isOn){
            isOn=false;
        }else{
            isOn=true;
        }
    }
}
