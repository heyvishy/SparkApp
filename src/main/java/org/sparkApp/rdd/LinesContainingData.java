package org.sparkApp.rdd;

import java.io.InputStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class LinesContainingData {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String file = "/Users/vishalshukla/Documents/workspace-sts-3.4.0.RELEASE/SparkApp/src/main/resources/testfile.txt";
	    SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaRDD<String> textFile = sc.textFile(file).cache();
	    
	    Function<String, Boolean> function = new Function<String, Boolean>(){
			public Boolean call(String line) throws Exception {
				return line.contains("for");
			}
	    };
	    
		JavaRDD<String> dataLines = textFile.filter(function);
	    System.out.println("dataLines count "+dataLines.count());
	}

}
