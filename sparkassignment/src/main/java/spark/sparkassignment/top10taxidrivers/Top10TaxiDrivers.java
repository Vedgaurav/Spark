package spark.sparkassignment.top10taxidrivers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import spark.sparkassignment.bean.TaxiBean;
import spark.sparkassignment.configproperties.ConfigProperties;

/**
 * @author Ved
 * 
 *         city wise they top 10 taxi drivers.
 *
 */
public class Top10TaxiDrivers {

	static final Map<String, Integer> partitionMap = new HashMap<String, Integer>();
	static int partitionIndex = -1;

	public static void main(String[] args) {

		SparkConf configuration = new SparkConf();

		ConfigProperties configProperties = new ConfigProperties();

		configuration.setMaster(configProperties.getMasterMode())
				.setAppName(configProperties.getAppNameForTop10TaxiDrivers());

		JavaSparkContext sparkContext = new JavaSparkContext(configuration);

		/**
		 * Loads the file. Returns city,pickup Timestamp, taxi driver name, customer
		 * review and count of the record
		 */
		JavaPairRDD<String, Tuple2<Integer, Integer>> textLoadRDD = sparkContext
				.textFile(configProperties.getInputFile())

				.mapToPair(new PairFunction<String, String, Tuple2<Integer, Integer>>() {

					private static final long serialVersionUID = 239830850135024285L;

					public Tuple2<String, Tuple2<Integer, Integer>> call(String records) throws Exception {

						TaxiBean taxiBean = new TaxiBean();

						taxiBean.setTaxi_driver_name(records.split(",")[0]);

						taxiBean.setPickup_ts(records.split(",")[2]);

						taxiBean.setCity(records.split(",")[7]);

						taxiBean.setCustomer_review(Integer.parseInt(records.split(",")[11]));

						return new Tuple2<String, Tuple2<Integer, Integer>>(
								taxiBean.getCity() + " " + taxiBean.getPickup_ts().split("-")[0] + " "
										+ taxiBean.getTaxi_driver_name(),
								new Tuple2<Integer, Integer>(taxiBean.getCustomer_review(), 1));
					}

				});

		/** Adds the count by key and also ratings */
		JavaPairRDD<String, Tuple2<Integer, Integer>> reducedByKeyRDD = textLoadRDD.reduceByKey(
				new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {

					private static final long serialVersionUID = 1877106258020046693L;

					@Override
					public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> value1,
							Tuple2<Integer, Integer> value2) throws Exception {

						return new Tuple2<Integer, Integer>(value1._1() + value2._1(), value1._2() + value2._2());
					}
				});

		/**
		 * Takes out average of the ratings and also makes that as key. Sorts on it.key
		 * by city and year
		 */
		JavaPairRDD<String, Tuple2<Double, String>> pairRDD = reducedByKeyRDD
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, Double, String>() {

					private static final long serialVersionUID = -6711266267615106570L;

					@Override
					public Tuple2<Double, String> call(Tuple2<String, Tuple2<Integer, Integer>> tuple)
							throws Exception {

						double averageOfRatings = tuple._2()._1() / tuple._2()._2();

						return new Tuple2<Double, String>(averageOfRatings, tuple._1());
					}

				}).sortByKey(false).keyBy(f -> f._2().split(" ")[0] + " " + f._2().split(" ")[1]);

		int noOfKeys = (int) pairRDD.keys().distinct().count();

		/** Partitions the RDD based on key city and year */
		JavaPairRDD<String, Tuple2<Double, String>> partitionedRDD = pairRDD.partitionBy(new Partitioner() {

			private static final long serialVersionUID = -6075473636065046431L;

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

		/** Finds out top 10 records from each partition. */
		JavaRDD<Tuple2<String, Double>> top10RDD = partitionedRDD.mapPartitions(
				new FlatMapFunction<Iterator<Tuple2<String, Tuple2<Double, String>>>, Tuple2<String, Double>>() {

					private static final long serialVersionUID = 4744932234171246415L;

					@Override
					public Iterator<Tuple2<String, Double>> call(
							Iterator<Tuple2<String, Tuple2<Double, String>>> iterator) throws Exception {

						ArrayList<Tuple2<String, Double>> list = new ArrayList<Tuple2<String, Double>>();

						Tuple2<String, Double> newTuple;

						for (int i = 0; i < 10 && iterator.hasNext(); i++) {

							newTuple = new Tuple2<String, Double>(iterator.next()._2()._2(), iterator.next()._2()._1());

							list.add(newTuple);
						}

						return list.iterator();

					}

				});

		top10RDD.foreach(record -> System.out.println(record));

		top10RDD.saveAsTextFile(configProperties.getTop10TaxiDriversOutputLocation());

		sparkContext.close();

	}

}
