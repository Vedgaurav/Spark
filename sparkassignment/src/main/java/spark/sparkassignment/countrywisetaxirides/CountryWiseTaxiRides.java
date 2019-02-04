package spark.sparkassignment.countrywisetaxirides;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import spark.sparkassignment.bean.TaxiBean;
import spark.sparkassignment.configproperties.ConfigProperties;

/**
 * @author Ved
 * 
 *         Country wise count the number of taxi rides, order them from highest
 *         to lowest.
 *
 */
public class CountryWiseTaxiRides {

	public static void main(String[] args) {

		SparkConf configuration = new SparkConf();

		ConfigProperties configProperties = new ConfigProperties();

		configuration.setMaster(configProperties.getMasterMode())
				.setAppName(configProperties.getAppNameForCountryWiseTaxiRides());

		JavaSparkContext sparkContext = new JavaSparkContext(configuration);

		/** Load the file and return the country name and each record count (as 1) */
		JavaPairRDD<String, Long> textLoadRDD = sparkContext.textFile(configProperties.getInputFile())
				.mapToPair(new PairFunction<String, String, Long>() {

					private static final long serialVersionUID = 7451646322476530573L;

					@Override
					public Tuple2<String, Long> call(String record) throws Exception {

						TaxiBean taxiBean = new TaxiBean();

						taxiBean.setCountry(record.split(",")[9]);

						return new Tuple2<String, Long>(taxiBean.getCountry(), (long) 1);
					}
				});

		/** Adds the no of records for each country */
		JavaPairRDD<String, Long> reducedRDD = textLoadRDD.reduceByKey((value1, value2) -> value1 + value2);

		/**
		 * Swaps key value. key as sum of records and value as country. Then sorts by
		 * key
		 */
		JavaPairRDD<Long, String> swappedPairRDD = reducedRDD
				.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {

					private static final long serialVersionUID = -915620001436583393L;

					@Override
					public Tuple2<Long, String> call(Tuple2<String, Long> tuple) throws Exception {

						return tuple.swap();
					}
				}).sortByKey(false);

		swappedPairRDD.foreach(record -> System.out.println(record._1() + " " + record._2()));

		//swappedPairRDD.saveAsTextFile(configProperties.getCountryWiseTaxiRidesOutputLocation());

		sparkContext.close();

	}

}
