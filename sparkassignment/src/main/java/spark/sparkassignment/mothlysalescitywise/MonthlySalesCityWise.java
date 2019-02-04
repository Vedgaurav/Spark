package spark.sparkassignment.mothlysalescitywise;

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
 *         Monthly sales for each city.
 *
 */
public class MonthlySalesCityWise {

	static final Map<String, Integer> partitionMap = new HashMap<String, Integer>();
	static int partitionIndex = -1;

	public static void main(String[] args) {
		SparkConf configuration = new SparkConf();

		ConfigProperties configProperties = new ConfigProperties();

		configuration.setMaster(configProperties.getMasterMode())
				.setAppName(configProperties.getAppNameForMonthlySalesCityWise());

		JavaSparkContext sparkContext = new JavaSparkContext(configuration);

		/** Loads the data and returns city year month as key and fare as value. */
		JavaPairRDD<String, Double> textLoadRDD = sparkContext
				.textFile(configProperties.getInputFile())
				.mapToPair(new PairFunction<String, String, Double>() {

					private static final long serialVersionUID = -6521546177147615456L;

					@Override
					public Tuple2<String, Double> call(String record) throws Exception {

						TaxiBean taxiBean = new TaxiBean();

						taxiBean.setPickup_ts(record.split(",")[2]);
						taxiBean.setCity(record.split(",")[7]);
						taxiBean.setFare(Double.parseDouble(record.split(",")[6]));

						String yearMonth = taxiBean.getPickup_ts().split(" ")[0].substring(0, 7);

						return new Tuple2<String, Double>(taxiBean.getCity() + " " + yearMonth, taxiBean.getFare());
					}

				});

		/** Adds the fare. */
		JavaPairRDD<String, Double> sumRDD = textLoadRDD.reduceByKey((value1, value2) -> value1 + value2);

		/** Key by by city and year. */
		JavaPairRDD<String, Tuple2<String, Double>> keyByRDD = sumRDD
				.keyBy(key -> key._1().split(" ")[0] + key._1().split(" ")[1].substring(0, 4));

		int noOfKeys = (int) sumRDD.keys().distinct().count();

		/** Partitions on the basis of key. */
		JavaPairRDD<String, Tuple2<String, Double>> partitionedRDD = keyByRDD.partitionBy(new Partitioner() {

			private static final long serialVersionUID = 1197334176754244179L;

			public int numPartitions() {

				return noOfKeys;
			}

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

		partitionedRDD.saveAsTextFile(configProperties.getMonthlySalesCityWiseOutputLocation());

		sparkContext.close();

	}
}
