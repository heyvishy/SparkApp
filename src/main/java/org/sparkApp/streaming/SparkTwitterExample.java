package org.sparkApp.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

public class SparkTwitterExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf =  new SparkConf().setAppName("SparkTwitterExample").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);		
		//Create Streaming context with n-seconds bacth size
		JavaStreamingContext jssc = new JavaStreamingContext(sc,Durations.seconds(5));
//		// Create DStream from all the input on port 7777
//		JavaDStream<String> lines = jssc.socketTextStream("localhost", 9999);
//		//TwitterUtils

	    // Checkpoint directory
	    //String checkpointDir = TutorialHelper.getCheckpointDirectory();

	    // Configuring Twitter credentials
	    String apiKey = "Asu2oPLikpk9d9D9Kjf2WP7Z1";
	    String apiSecret = "0jCZzL6DBA0ZKv1mlo71wxepYH1WlX5Y3aeJaGJmJcQx5PyYt3";
	    String accessToken = "2340447961-z8FjcGh9znRDvrGZmBWaiWNHa0fxXIN84AL9zId";
	    String accessTokenSecret = "7caiUWGHAn1CiySr0rVk6UspiPL82DE6MteGkYVOuMWpQ";
	    //TwitterUtils. .configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret);
	    
	    
	    TwitterUtils.createStream(jssc);
	    // Your code goes here
	}

}
