package org.sparkApp.util;

import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterUtil {

	public static void main(String[] args) throws TwitterException {
		// TODO Auto-generated method stub
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("Asu2oPLikpk9d9D9Kjf2WP7Z1")
		  .setOAuthConsumerSecret("0jCZzL6DBA0ZKv1mlo71wxepYH1WlX5Y3aeJaGJmJcQx5PyYt3")
		  .setOAuthAccessToken("2340447961-Yx4434WuJkboTslb5AQYasv3dD5xSU7CdjtygZP")
		  .setOAuthAccessTokenSecret("1sr0RCXKqhqqg0jCEJHEjiFryg49WPS0nLbeABR5xpt36");
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
		Status status = twitter.updateStatus("test");
	    System.out.println("Successfully updated the status to [" + status.getText() + "].");
	}

}
