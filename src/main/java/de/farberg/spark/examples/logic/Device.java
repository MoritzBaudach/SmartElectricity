package de.farberg.spark.examples.logic;

/**
 * Created by krischke on 27.07.2016.
 */
public class Device {

    private String id;
    private double energyConsumption;
    private boolean isOn;
    private boolean wasOn;
    private boolean transitionState;
    private int endCount;

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
        }}


    public void setEndCount(int endCount){
        this.endCount=endCount;
    }

    public int getEndCount(){
        return this.endCount;
    }

    public boolean getWasOn(){
        return this.wasOn;
    }

    public void switchWasOn(){
        if(wasOn){
            wasOn=false;
        }else{
            wasOn=true;
        }}}
