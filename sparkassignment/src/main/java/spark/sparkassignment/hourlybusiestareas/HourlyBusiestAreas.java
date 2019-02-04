package spark.sparkassignment.hourlybusiestareas;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.Partitioner;
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
 *         Hourly which areas are the busiest in terms of taxi rides.
 *
 */
public class HourlyBusiestAreas {

	static final Map<String, Integer> partitionMap = new HashMap<String, Integer>();
	static int partitionIndex = -1;

	public static void main(String[] args) {

		SparkConf configuration = new SparkConf();

		ConfigProperties configProperties = new ConfigProperties();

		configuration.setMaster(configProperties.getMasterMode())
				.setAppName(configProperties.getAppNameForHourlyBusiestAreas());

		JavaSparkContext sparkContext = new JavaSparkContext(configuration);

		/**
		 * Loads the file and returns city, hour and pickup location and count 1. Add
		 * the hour value
		 */
		JavaPairRDD<String, Integer> textLoadRDD = sparkContext
				.textFile(configProperties.getInputFile())

				.mapToPair(new PairFunction<String, String, Integer>() {

					private static final long serialVersionUID = 1254492636291822260L;

					public Tuple2<String, Integer> call(String records) throws Exception {

						TaxiBean taxiBean = new TaxiBean();

						taxiBean.setPickup_loc(records.split(",")[1]);
						taxiBean.setPickup_ts(records.split(",")[2]);

						taxiBean.setCity(records.split(",")[7]);

						return new Tuple2<String, Integer>(taxiBean.getCity() + " "
								+ taxiBean.getPickup_ts().split("-")[1] + " " + taxiBean.getPickup_loc(), 1);

					}

				}).reduceByKey((value1, value2) -> value1 + value2);

		/** Swaps the key and value and sort by key in descending order */
		JavaPairRDD<Integer, String> sortedRDD = textLoadRDD.mapToPair(tuple -> tuple.swap()).sortByKey(false);

		/** Creates new key by city */
		JavaPairRDD<String, Tuple2<Integer, String>> newKeyByRDD = sortedRDD.keyBy(key -> key._2.split(" ")[0]);

		int noOfKeys = (int) newKeyByRDD.keys().distinct().count();

		/** Partitions on the basis of city name. */
		JavaPairRDD<String, Tuple2<Integer, String>> partitionedRDD = newKeyByRDD.partitionBy(new Partitioner() {

			private static final long serialVersionUID = 6410556761058210857L;

			@Override
			public int numPartitions() {

				return noOfKeys;
			}

			@Override
			public int getPartition(Object arg0) {

				if (!arg0.equals(null)) {
					if (!arg0.toString().equals(" ")) {

						if (partitionMap.containsKey(arg0.toString())) {

							return partitionMap.get(arg0.toString());
						} else
							partitionMap.put(arg0.toString(), ++partitionIndex);

					}
				}
				return partitionMap.get(arg0.toString());
			}

		});

		partitionedRDD.foreach(record -> System.out.println(record));

		partitionedRDD.saveAsTextFile(configProperties.getHourlyBusiestAreasOutputLocation());

		sparkContext.close();

	}

}
