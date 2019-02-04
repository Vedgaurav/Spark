package spark.sparkassignment.mostttimetocommute;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import spark.sparkassignment.bean.TaxiBean;
import spark.sparkassignment.configproperties.ConfigProperties;

/**
 * @author Ved
 * 
 *         In each city which areas take the most time to commute.
 *
 */
public class MostTimeToCommute {

	static final Map<String, Integer> map = new HashMap<String, Integer>();
	static int count = -1;

	/**
	 * @param dateTime
	 * @return
	 * 
	 * 		Takes timestamp value as input and gives time as output in long
	 *         format.
	 */
	public static long toMilliSeconds(String dateTime) {

		Date date = null;

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		try {

			date = dateFormat.parse(dateTime);

		} catch (ParseException e) {

			System.out.println(e.getMessage());

		}

		return date.getTime();
	}

	public static void main(String[] args) {

		SparkConf configuration = new SparkConf();

		ConfigProperties configProperties = new ConfigProperties();

		configuration.setMaster(configProperties.getMasterMode())
				.setAppName(configProperties.getAppNameForMostTimeToCommute());

		JavaSparkContext sparkContext = new JavaSparkContext(configuration);

		/**
		 * Load the input file and returns city time difference between pickup and drop,
		 * pickup and drop locations, count of that record
		 */
		JavaPairRDD<String, Tuple2<Long, Integer>> textLoadRDD = sparkContext
				.textFile(configProperties.getInputFile())

				.mapToPair(new PairFunction<String, String, Tuple2<Long, Integer>>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, Tuple2<Long, Integer>> call(String records) throws Exception {

						TaxiBean taxiBean = new TaxiBean();

						taxiBean.setPickup_loc(records.split(",")[1]);
						taxiBean.setPickup_ts(records.split(",")[2]);
						taxiBean.setDrop_loc(records.split(",")[3]);
						taxiBean.setDrop_ts(records.split(",")[4]);

						taxiBean.setCity(records.split(",")[7]);

						long timeDifference = toMilliSeconds(taxiBean.getDrop_ts())
								- toMilliSeconds(taxiBean.getPickup_ts());

						return new Tuple2<String, Tuple2<Long, Integer>>(
								taxiBean.getCity() + " " + taxiBean.getPickup_loc() + " " + taxiBean.getDrop_loc(),
								new Tuple2<Long, Integer>(timeDifference, 1));
					}

				});

		/** Adds the time difference and it's count. */
		JavaPairRDD<String, Tuple2<Long, Integer>> reducedByKeyRDD = textLoadRDD
				.reduceByKey(new Function2<Tuple2<Long, Integer>, Tuple2<Long, Integer>, Tuple2<Long, Integer>>() {

					private static final long serialVersionUID = 8170476503750111420L;

					public Tuple2<Long, Integer> call(Tuple2<Long, Integer> value1, Tuple2<Long, Integer> value2)
							throws Exception {

						return new Tuple2<Long, Integer>(value1._1() + value2._1(), value1._2() + value2._2());
					}
				});

		/** Average the time difference */
		JavaPairRDD<String, Tuple2<Double, String>> pairRDD = reducedByKeyRDD
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Long, Integer>>, Double, String>() {

					private static final long serialVersionUID = 4684594817359512025L;

					public Tuple2<Double, String> call(Tuple2<String, Tuple2<Long, Integer>> tuple) throws Exception {

						double averageTimeDifference = tuple._2()._1() / tuple._2()._2();

						return new Tuple2<Double, String>(averageTimeDifference, tuple._1());
					}

				}).sortByKey(false).keyBy(key -> key._2().split(" ")[0]);

		int noOfKeys = (int) pairRDD.keys().distinct().count();

		/** partitions according to city */
		JavaPairRDD<String, Tuple2<Double, String>> partitionedRDD = pairRDD.partitionBy(new Partitioner() {

			private static final long serialVersionUID = 1080624495089924360L;

			@Override
			public int numPartitions() {

				return noOfKeys;
			}

			@Override
			public int getPartition(Object arg0) {

				if (!arg0.equals(null)) {
					if (!arg0.toString().equals(" ")) {

						if (map.containsKey(arg0.toString())) {

							return map.get(arg0.toString());
						} else
							map.put(arg0.toString(), ++count);

					}
				}
				return map.get(arg0.toString());
			}

		});

		partitionedRDD.foreach(record -> System.out.println(record));

		partitionedRDD.saveAsTextFile(configProperties.getMostTimeToCommuteOutputLocation());

		sparkContext.close();

	}

}
