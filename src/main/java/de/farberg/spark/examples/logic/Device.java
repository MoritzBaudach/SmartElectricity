package de.farberg.spark.examples.logic;

/**
 * Created by krischke on 27.07.2016.
 */
public class Device {

    private String id;
    private int energyConsumption;
    private boolean ready;
    private boolean finished;

    public Device(String id, int energyConsumption, boolean ready){
        this.setId(id);
        this.setEnergyConsumption(energyConsumption);
        this.setReady(ready);
        this.setFinished(false);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getEnergyConsumption() {
        return energyConsumption;
    }

    public void setEnergyConsumption(int energyConsumption) {
        this.energyConsumption = energyConsumption;
    }

    public boolean isReady() {
        return ready;
    }

    public void setReady(boolean ready) {
        this.ready = ready;
    }

    public boolean isFinished() {
        return finished;
    }

    public void setFinished(boolean finished) {
        this.finished = finished;
    }
}
