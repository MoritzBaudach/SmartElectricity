package de.farberg.spark.examples.logic;

/**
 * Created by krischke on 27.07.2016.
 */
public class Device {

    private String id;
    private double energyConsumption;
    private boolean isOn;
    private boolean wasOn;

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

    public void consumeEnergy(double consumption){
       // if(energyConsumption>consumption){
            energyConsumption-=consumption;
       // }else{
        //    setEnergyConsumption(0);
       // }
    }


    public boolean isOn() {
        return isOn;
    }

    public void switchState() {
        if(isOn){
            isOn=false;
        }else{
            isOn=true;
        }}


    public boolean getWasOn(){
        return this.wasOn;
    }

    public void switchWasOn(){
        if(wasOn){
            wasOn=false;
        }else{
            wasOn=true;
        }}}
