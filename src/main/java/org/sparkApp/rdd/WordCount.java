package org.sparkApp.rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		
		//String file ="hdfs://localhost:9000/user/vishalshukla/input/testfile.txt";
		String file = "src/main/resources/testfile.txt";
	    SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaRDD<String> textFile = sc.textFile(file).cache();
	    
		JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String lines) throws Exception {
				return Arrays.asList(lines.split(" "));
			}
		});
		
		JavaPairRDD<String, Integer> wordMap = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String,Integer>(t,1);
			}
		});
		
		JavaPairRDD<String,Integer> wordCount = wordMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		wordCount.saveAsTextFile("/Users/vishalshukla/Documents/workspace-sts-3.4.0.RELEASE/SparkApp/src/main/resources/output/words.txt");
	}

}
