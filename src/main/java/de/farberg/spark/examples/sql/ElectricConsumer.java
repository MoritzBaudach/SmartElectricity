package de.farberg.spark.examples.sql;

import java.io.Serializable;

public class ElectricConsumer implements Serializable {
	private static final long serialVersionUID = 1L;

	boolean switchedOn;
	int kwH;
	double posLon;
	double posLat;

	public ElectricConsumer(int kwH, double posLon, double posLat, boolean an) {
		super();
		this.kwH = kwH;
		this.posLon = posLon;
		this.posLat = posLat;
		this.switchedOn = an;
	}

	public boolean isSwitchedOn() {
		return switchedOn;
	}

	public void setSwitchedOn(boolean switchedOn) {
		this.switchedOn = switchedOn;
	}

	public int getKwH() {
		return kwH;
	}

	public void setKwH(int kwH) {
		this.kwH = kwH;
	}

	public double getPosLon() {
		return posLon;
	}

	public void setPosLon(double posLon) {
		this.posLon = posLon;
	}

	public double getPosLat() {
		return posLat;
	}

	public void setPosLat(double posLat) {
		this.posLat = posLat;
	}

}
