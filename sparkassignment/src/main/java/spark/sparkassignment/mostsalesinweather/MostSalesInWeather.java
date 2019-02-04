package spark.sparkassignment.mostsalesinweather;

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
 *         Most sales weather wise.
 *
 */
public class MostSalesInWeather {

	static final Map<String, Integer> partitionMap = new HashMap<String, Integer>();
	static int partitionIndex = -1;

	public static void main(String[] args) {

		SparkConf configuration = new SparkConf();

		ConfigProperties configProperties = new ConfigProperties();

		configuration.setMaster(configProperties.getMasterMode())
				.setAppName(configProperties.getAppNameForMostSalesInWeather());

		JavaSparkContext sparkContext = new JavaSparkContext(configuration);

		/**
		 * Loads the data and returns city weather as key and fare as value.Also adds
		 * the values of same key.
		 */
		JavaPairRDD<String, Double> textLoadRDD = sparkContext
				.textFile(configProperties.getInputFile())

				.mapToPair(new PairFunction<String, String, Double>() {

					private static final long serialVersionUID = 7416343597562539239L;

					@Override
					public Tuple2<String, Double> call(String record) throws Exception {

						TaxiBean taxiBean = new TaxiBean();

						taxiBean.setCity(record.split(",")[7]);
						taxiBean.setWeather(record.split(",")[10]);
						taxiBean.setFare(Double.parseDouble(record.split(",")[6]));

						return new Tuple2<String, Double>(taxiBean.getCity() + " " + taxiBean.getWeather(),
								taxiBean.getFare());
					}

				}).reduceByKey((value1, value2) -> value1 + value2);

		/** Makes fare a key and sorts it. */
		JavaPairRDD<Double, Tuple2<String, Double>> keyByFareRDD = textLoadRDD.keyBy(key -> key._2()).sortByKey(false);

		/** Makes new key as city wise. */
		JavaPairRDD<String, Tuple2<String, Double>> newKeyValueRDD = keyByFareRDD
				.mapToPair(new PairFunction<Tuple2<Double, Tuple2<String, Double>>, String, Tuple2<String, Double>>() {

					private static final long serialVersionUID = -4509796367900976650L;

					@Override
					public Tuple2<String, Tuple2<String, Double>> call(Tuple2<Double, Tuple2<String, Double>> tuple)
							throws Exception {

						return new Tuple2<String, Tuple2<String, Double>>(tuple._2()._1().split(" ")[0],
								new Tuple2<String, Double>(tuple._2()._1().split(" ")[1], tuple._2()._2()));
					}

				});

		int noOfKeys = (int) newKeyValueRDD.keys().distinct().count();

		/** Partition on the basis of keys. */
		JavaPairRDD<String, Tuple2<String, Double>> partitionedRDD = newKeyValueRDD.partitionBy(new Partitioner() {

			private static final long serialVersionUID = 5956333228441110120L;

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

		partitionedRDD.saveAsTextFile(configProperties.getMostSalesInWeatherOutputLocation());

		sparkContext.close();

	}

}
