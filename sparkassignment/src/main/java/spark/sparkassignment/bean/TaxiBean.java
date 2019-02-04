package spark.sparkassignment.bean;

import java.io.Serializable;

public class TaxiBean implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6257977391575364109L;
	
	String taxi_driver_name;
	String pickup_loc;
	String pickup_ts;
	String drop_loc;
	String drop_ts;
	int no_of_passengers;
	double fare;
	String city;
	String state;
	String country;
	String weather;
	int customer_review;
	
	
	public String getTaxi_driver_name() {
		return taxi_driver_name;
	}



	public void setTaxi_driver_name(String taxi_driver_name) {
		this.taxi_driver_name = taxi_driver_name;
	}



	public String getPickup_loc() {
		return pickup_loc;
	}



	public void setPickup_loc(String pickup_loc) {
		this.pickup_loc = pickup_loc;
	}



	public String getPickup_ts() {
		return pickup_ts;
	}



	public void setPickup_ts(String pickup_ts) {
		this.pickup_ts = pickup_ts;
	}



	public String getDrop_loc() {
		return drop_loc;
	}



	public void setDrop_loc(String drop_loc) {
		this.drop_loc = drop_loc;
	}



	public String getDrop_ts() {
		return drop_ts;
	}



	public void setDrop_ts(String drop_ts) {
		this.drop_ts = drop_ts;
	}



	public int getNo_of_passengers() {
		return no_of_passengers;
	}



	public void setNo_of_passengers(int no_of_passengers) {
		this.no_of_passengers = no_of_passengers;
	}



	public double getFare() {
		return fare;
	}



	public void setFare(double fare) {
		this.fare = fare;
	}



	public String getCity() {
		return city;
	}



	public void setCity(String city) {
		this.city = city;
	}



	public String getState() {
		return state;
	}



	public void setState(String state) {
		this.state = state;
	}



	public String getCountry() {
		return country;
	}



	public void setCountry(String country) {
		this.country = country;
	}



	public String getWeather() {
		return weather;
	}



	public void setWeather(String weather) {
		this.weather = weather;
	}



	public int getCustomer_review() {
		return customer_review;
	}



	public void setCustomer_review(int customer_review) {
		this.customer_review = customer_review;
	}

}
