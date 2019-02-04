package spark.sparkassignment.configproperties;

/**
 * @author Ved
 * 
 * Contains MasterMode, input file location, appName and outputlocation for all use cases.
 *
 */
public class ConfigProperties {
	
	String masterMode = "local";
	String inputFile = "E:\\SparkAssignment\\dataset\\TaxiDataset2.txt";
	
	String appNameForLeastSalesInWeather = "SalesLessInWeather";
	String leastSalesInWeatherOutputLocation = "/home/gaurav/Desktop/SparkAssignment/Outputs/LeastSalesInWeatherOutput";
	
	String appNameForTop10Charges2 = "Top10Charges2";
	String top10Charges2OutputLocation = "/home/gaurav/Desktop/SparkAssignment/Outputs/Top10Charges2Output";
	
	String appNameForCountryWiseTaxiRides = "CountryWiseTaxiRides";
	String countryWiseTaxiRidesOutputLocation = "/home/gaurav/Desktop/SparkAssignment/Outputs/CountryWiseTaxiRidesOutput";
	
	String appNameForHourlyBusiestAreas = "HourlyBusiestAreas";
	String hourlyBusiestAreasOutputLocation = "/home/gaurav/Desktop/SparkAssignment/Outputs/HourlyBusiestAreasOutput";
	
	String appNameForMostSalesInWeather = "MostSalesInWeather";
	String mostSalesInWeatherOutputLocation = "/home/gaurav/Desktop/SparkAssignment/Outputs/MostSalesInWeatherOutput";
	
	String appNameForMostTimeToCommute = "MostTimeToCommute";
	String mostTimeToCommuteOutputLocation = "/home/gaurav/Desktop/SparkAssignment/Outputs/MostTimeToCommuteOutput";
	
	String appNameForMonthlySalesCityWise = "MonthlySalesCityWise";
	String monthlySalesCityWiseOutputLocation = "/home/gaurav/Desktop/SparkAssignment/Outputs/MonthlySalesCityWiseOutput";
	
	String appNameForTop10MostPassengers = "Top10MostPassengers";
	String top10MostPassengersOutputLocation = "/home/gaurav/Desktop/SparkAssignment/Outputs/Top10MostPassengersOutput";
	
	String appNameForTop10TaxiDrivers = "Top10TaxiDrivers";
	String top10TaxiDriversOutputLocation = "/home/gaurav/Desktop/SparkAssignment/Outputs/Top10TaxiDriversOutput";
	
	
	
	
	
	public String getMasterMode() {
		return masterMode;
	}
	public void setMasterMode(String masterMode) {
		this.masterMode = masterMode;
	}
	public String getInputFile() {
		return inputFile;
	}
	public void setInputFile(String inputFile) {
		this.inputFile = inputFile;
	}
	public String getAppNameForLeastSalesInWeather() {
		return appNameForLeastSalesInWeather;
	}
	public void setAppNameForLeastSalesInWeather(String appNameForLeastSalesInWeather) {
		this.appNameForLeastSalesInWeather = appNameForLeastSalesInWeather;
	}
	public String getLeastSalesInWeatherOutputLocation() {
		return leastSalesInWeatherOutputLocation;
	}
	public void setLeastSalesInWeatherOutputLocation(String leastSalesInWeatherOutputLocation) {
		this.leastSalesInWeatherOutputLocation = leastSalesInWeatherOutputLocation;
	}
	public String getAppNameForTop10Charges2() {
		return appNameForTop10Charges2;
	}
	public void setAppNameForTop10Charges2(String appNameForTop10Charges2) {
		this.appNameForTop10Charges2 = appNameForTop10Charges2;
	}
	public String getTop10Charges2OutputLocation() {
		return top10Charges2OutputLocation;
	}
	public void setTop10Charges2OutputLocation(String top10Charges2OutputLocation) {
		this.top10Charges2OutputLocation = top10Charges2OutputLocation;
	}
	public String getAppNameForCountryWiseTaxiRides() {
		return appNameForCountryWiseTaxiRides;
	}
	public void setAppNameForCountryWiseTaxiRides(String appNameForCountryWiseTaxiRides) {
		this.appNameForCountryWiseTaxiRides = appNameForCountryWiseTaxiRides;
	}
	public String getCountryWiseTaxiRidesOutputLocation() {
		return countryWiseTaxiRidesOutputLocation;
	}
	public void setCountryWiseTaxiRidesOutputLocation(String countryWiseTaxiRidesOutputLocation) {
		this.countryWiseTaxiRidesOutputLocation = countryWiseTaxiRidesOutputLocation;
	}
	public String getAppNameForHourlyBusiestAreas() {
		return appNameForHourlyBusiestAreas;
	}
	public void setAppNameForHourlyBusiestAreas(String appNameForHourlyBusiestAreas) {
		this.appNameForHourlyBusiestAreas = appNameForHourlyBusiestAreas;
	}
	public String getHourlyBusiestAreasOutputLocation() {
		return hourlyBusiestAreasOutputLocation;
	}
	public void setHourlyBusiestAreasOutputLocation(String hourlyBusiestAreasOutputLocation) {
		this.hourlyBusiestAreasOutputLocation = hourlyBusiestAreasOutputLocation;
	}
	public String getAppNameForMostSalesInWeather() {
		return appNameForMostSalesInWeather;
	}
	public void setAppNameForMostSalesInWeather(String appNameForMostSalesInWeather) {
		this.appNameForMostSalesInWeather = appNameForMostSalesInWeather;
	}
	public String getMostSalesInWeatherOutputLocation() {
		return mostSalesInWeatherOutputLocation;
	}
	public void setMostSalesInWeatherOutputLocation(String mostSalesInWeatherOutputLocation) {
		this.mostSalesInWeatherOutputLocation = mostSalesInWeatherOutputLocation;
	}
	public String getAppNameForMostTimeToCommute() {
		return appNameForMostTimeToCommute;
	}
	public void setAppNameForMostTimeToCommute(String appNameForMostTimeToCommute) {
		this.appNameForMostTimeToCommute = appNameForMostTimeToCommute;
	}
	public String getMostTimeToCommuteOutputLocation() {
		return mostTimeToCommuteOutputLocation;
	}
	public void setMostTimeToCommuteOutputLocation(String mostTimeToCommuteOutputLocation) {
		this.mostTimeToCommuteOutputLocation = mostTimeToCommuteOutputLocation;
	}
	public String getAppNameForMonthlySalesCityWise() {
		return appNameForMonthlySalesCityWise;
	}
	public void setAppNameForMonthlySalesCityWise(String appNameForMonthlySalesCityWise) {
		this.appNameForMonthlySalesCityWise = appNameForMonthlySalesCityWise;
	}
	public String getMonthlySalesCityWiseOutputLocation() {
		return monthlySalesCityWiseOutputLocation;
	}
	public void setMonthlySalesCityWiseOutputLocation(String monthlySalesCityWiseOutputLocation) {
		this.monthlySalesCityWiseOutputLocation = monthlySalesCityWiseOutputLocation;
	}
	
	public String getAppNameForTop10MostPassengers() {
		return appNameForTop10MostPassengers;
	}
	public void setAppNameForTop10MostPassengers(String appNameForTop10MostPassengers) {
		this.appNameForTop10MostPassengers = appNameForTop10MostPassengers;
	}
	
		public String getAppNameForTop10TaxiDrivers() {
		return appNameForTop10TaxiDrivers;
	}
	public void setAppNameForTop10TaxiDrivers(String appNameForTop10TaxiDrivers) {
		this.appNameForTop10TaxiDrivers = appNameForTop10TaxiDrivers;
	}
		public String getTop10TaxiDriversOutputLocation() {
		return top10TaxiDriversOutputLocation;
	}
	public void setTop10TaxiDriversOutputLocation(String top10TaxiDriversOutputLocation) {
		this.top10TaxiDriversOutputLocation = top10TaxiDriversOutputLocation;
	}
	public String getTop10MostPassengersOutputLocation() {
		return top10MostPassengersOutputLocation;
	}
	public void setTop10MostPassengersOutputLocation(String top10MostPassengersOutputLocation) {
		this.top10MostPassengersOutputLocation = top10MostPassengersOutputLocation;
	}
	
	
	
	

}
