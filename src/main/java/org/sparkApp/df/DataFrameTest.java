package org.sparkApp.df;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class DataFrameTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//String file = "/Users/vishalshukla/Documents/workspace-sts-3.4.0.RELEASE/SparkApp/src/main/resources/people.json";
	    SparkConf conf = new SparkConf().setAppName("DataFrameTest").setMaster("local");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

	    DataFrame df = sqlContext.read().json("/Users/vishalshukla/Documents/workspace-sts-3.4.0.RELEASE/SparkApp/src/main/resources/people.json");
	    
	    // Displays the content of the DataFrame to stdout
	    df.show();
	    // Print the schema in a tree format
	    df.printSchema();
	    // root
	    // |-- age: long (nullable = true)
	    // |-- name: string (nullable = true)

	    // Select only the "name" column
	    df.select("name").show();
	    // name
	    // Michael
	    // Andy
	    // Justin

	    // Select everybody, but increment the age by 1
	    df.select(df.col("name"), df.col("age").plus(1)).show();
	    // name    (age + 1)
	    // Michael null
	    // Andy    31
	    // Justin  20

	}

}
