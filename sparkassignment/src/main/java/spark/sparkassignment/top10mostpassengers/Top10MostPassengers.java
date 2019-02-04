package spark.sparkassignment.top10mostpassengers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import spark.sparkassignment.bean.TaxiBean;
import spark.sparkassignment.configproperties.ConfigProperties;

/**
 * @author Ved
 * 
 *         Top 10 taxi drives who took the most number of passengers per city.
 *
 */
public class Top10MostPassengers {

	static final Map<String, Integer> partitionMap = new HashMap<String, Integer>();
	static int partitionIndex = -1;

	public static void main(String[] args) {

		SparkConf configuration = new SparkConf();

		ConfigProperties configProperties = new ConfigProperties();

		configuration.setMaster(configProperties.getMasterMode())
				.setAppName(configProperties.getAppNameForTop10MostPassengers());

		JavaSparkContext sparkContext = new JavaSparkContext(configuration);

		/**
		 * Loads the data. and returns No of passengers and city name. Get distinct
		 * values of no of passengers and sorts it.
		 */
		JavaPairRDD<Integer, String> textLoadRDD = sparkContext
				.textFile(configProperties.getInputFile())

				.mapToPair(new PairFunction<String, Integer, String>() {

					private static final long serialVersionUID = 7884560413752507916L;

					public Tuple2<Integer, String> call(String record) throws Exception {

						TaxiBean taxiBean = new TaxiBean();

						taxiBean.setCity(record.split(",")[7]);
						taxiBean.setNo_of_passengers(Integer.parseInt(record.split(",")[5]));

						return new Tuple2<Integer, String>(taxiBean.getNo_of_passengers(), taxiBean.getCity());
					}
				}).distinct().sortByKey(false);

		/** Swap the key and values. */
		JavaPairRDD<String, Integer> swappedRDD = textLoadRDD.mapToPair(tuple -> tuple.swap());

		int noOfKeys = (int) swappedRDD.keys().distinct().count();

		System.out.println("noofkeys " + noOfKeys);

		/** Partition on the basis of city. */
		JavaPairRDD<String, Integer> partitionRDD = swappedRDD.partitionBy(new Partitioner() {

			private static final long serialVersionUID = 1651563472493653982L;

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

		/** Get top 10 values from each partition. */
		JavaPairRDD<String, Integer> top10RDD = partitionRDD
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Integer>>, String, Integer>() {

					private static final long serialVersionUID = -4194343048370651714L;

					public Iterator<Tuple2<String, Integer>> call(Iterator<Tuple2<String, Integer>> iterator)
							throws Exception {

						ArrayList<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();

						for (int i = 0; i < 3 && iterator.hasNext(); i++) {

							list.add(iterator.next());
						}

						return list.iterator();
					}
				});

		top10RDD.foreach(record -> System.out.println(record));

		top10RDD.saveAsTextFile(configProperties.getTop10MostPassengersOutputLocation());

		sparkContext.close();

	}

}
