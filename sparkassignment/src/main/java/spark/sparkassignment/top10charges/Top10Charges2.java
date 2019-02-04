package spark.sparkassignment.top10charges;

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
 *         Top 10 most charged taxi rides for each city.
 *
 */
public class Top10Charges2 {

	static final Map<String, Integer> partitionMap = new HashMap<String, Integer>();
	static int partitionIndex = -1;

	public static void main(String[] args) {

		SparkConf configuration = new SparkConf();

		ConfigProperties configProperties = new ConfigProperties();

		configuration.setMaster(configProperties.getMasterMode())
				.setAppName(configProperties.getAppNameForTop10Charges2());

		JavaSparkContext sparkContext = new JavaSparkContext(configuration);

		/**
		 * Loads the data from file and mapsToPair by Fare as key and city as value.
		 * Sorts by distinct keys(fares)
		 */
		/*
		 * JavaPairRDD<Double, String> textLoadRDD = sparkContext
		 * .textFile("/home/gaurav/Desktop/SparkAssignment/TaxiDataset2.txt")
		 */

		JavaPairRDD<Double, String> textLoadRDD = sparkContext
				.textFile(configProperties.getInputFile())
				.mapToPair(new PairFunction<String, Double, String>() {

					private static final long serialVersionUID = -5498002260395264784L;

					public Tuple2<Double, String> call(String record) throws Exception {

						TaxiBean taxiBean = new TaxiBean();

						taxiBean.setCity(record.split(",")[7]);
						taxiBean.setFare(Double.parseDouble(record.split(",")[6]));

						return new Tuple2<Double, String>(taxiBean.getFare(), taxiBean.getCity());
					}
				}).distinct().sortByKey(false);

		/** Swaps the keys and values. City as key and fare as value. */
		JavaPairRDD<String, Double> swappedRDD = textLoadRDD
				.mapToPair(new PairFunction<Tuple2<Double, String>, String, Double>() {

					private static final long serialVersionUID = -5320993382849149708L;

					public Tuple2<String, Double> call(Tuple2<Double, String> tuple) throws Exception {

						return tuple.swap();
					}
				});

		int noOfKeys = (int) swappedRDD.keys().distinct().count();

		/** Partitions by keys(city) */
		JavaPairRDD<String, Double> partitionRDD = swappedRDD.partitionBy(new Partitioner() {

			private static final long serialVersionUID = 91638704746413467L;

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

		/** Takes top 10 values from every partition */
		JavaPairRDD<String, Double> top10RecordsRDD = partitionRDD
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Double>>, String, Double>() {

					private static final long serialVersionUID = -3268952764111848092L;

					public Iterator<Tuple2<String, Double>> call(Iterator<Tuple2<String, Double>> iterator)
							throws Exception {

						ArrayList<Tuple2<String, Double>> list = new ArrayList<Tuple2<String, Double>>();

						for (int i = 0; i < 10 && iterator.hasNext(); i++) {

							list.add(iterator.next());
						}

						return list.iterator();
					}
				});

		top10RecordsRDD.foreach(f -> System.out.println(f));

		System.out.println("Partition Count " + top10RecordsRDD.getNumPartitions());

		top10RecordsRDD.saveAsTextFile(configProperties.getTop10Charges2OutputLocation());

		sparkContext.close();

	}

}
