package org.sparkApp.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * This is simple program utilizes Spark Streaming to filter error lines coming from streaming data.
 * @author vishalshukla
 *
 */
public class FilterLogLines {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("FilterLogLines").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Create Streaming context with n-seconds bacth size
		JavaStreamingContext jssc = new JavaStreamingContext(sc,Durations.seconds(5));
		// Create DStream from all the input on port 7777
		JavaDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		//Filter DStream for lines with  "error"
		JavaDStream<String> errorLines = lines.filter(new Function<String, Boolean>(){
			public Boolean call(String line) throws Exception {
				// TODO Auto-generated method stub
				return line.contains("error");
			}
		});
		
		//print out lines with error
		errorLines.print();
		//kick start streaming
		jssc.start();
		jssc.awaitTermination();
		
	}

}
