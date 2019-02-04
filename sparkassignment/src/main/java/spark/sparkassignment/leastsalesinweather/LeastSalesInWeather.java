package spark.sparkassignment.leastsalesinweather;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import spark.sparkassignment.configproperties.ConfigProperties;

/**
 * @author Ved
 * 
 *         In which weather least sales are there city wise
 *
 */
public class LeastSalesInWeather {

	static final Map<String, Integer> partitionMap = new HashMap<String, Integer>();
	static int partitionIndex = -1;

	public static void main(String[] args) {

		SparkConf configuration = new SparkConf();

		ConfigProperties configProperties = new ConfigProperties();

		configuration.setMaster(configProperties.getMasterMode())
				.setAppName(configProperties.getAppNameForLeastSalesInWeather());

		JavaSparkContext sparkContext = new JavaSparkContext(configuration);

		JavaPairRDD<String, Double> textLoadRDD = sparkContext
				.textFile(configProperties.getInputFile())

				.mapToPair(new PairFunction<String, String, Double>() {

					private static final long serialVersionUID = 3966285311713755363L;

					public Tuple2<String, Double> call(String record) throws Exception {

						return new Tuple2<String, Double>(record.split(",")[7] + " " + record.split(",")[10],
								Double.parseDouble(record.split(",")[6]));
					}

				}).reduceByKey((value1, value2) -> value1 + value2);

		JavaPairRDD<Double, Tuple2<String, Double>> keyByFareRDD = textLoadRDD.keyBy(key -> key._2()).sortByKey(true);

		JavaPairRDD<String, Tuple2<String, Double>> newKeyValueRDD = keyByFareRDD
				.mapToPair(new PairFunction<Tuple2<Double, Tuple2<String, Double>>, String, Tuple2<String, Double>>() {

					private static final long serialVersionUID = 8218759393252745831L;

					@Override
					public Tuple2<String, Tuple2<String, Double>> call(Tuple2<Double, Tuple2<String, Double>> tuple)
							throws Exception {

						return new Tuple2<String, Tuple2<String, Double>>(tuple._2()._1().split(" ")[0],
								new Tuple2<String, Double>(tuple._2()._1().split(" ")[1], tuple._2()._2()));
					}

				});

		int noOfKeys = (int) newKeyValueRDD.keys().distinct().count();

		JavaPairRDD<String, Tuple2<String, Double>> partitionedRDD = newKeyValueRDD.partitionBy(new Partitioner() {

			private static final long serialVersionUID = 3260019513175982843L;

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

		// partitionedRDD.saveAsTextFile(configProperties.getLeastSalesInWeatherOutputLocation());

		sparkContext.close();

	}

}
