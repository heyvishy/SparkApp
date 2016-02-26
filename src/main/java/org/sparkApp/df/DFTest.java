package org.sparkApp.df;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class DFTest {

	public static class Person implements Serializable {
		  private String name;
		  private int age;

		  public String getName() {
		    return name;
		  }

		  public void setName(String name) {
		    this.name = name;
		  }

		  public int getAge() {
		    return age;
		  }

		  public void setAge(int age) {
		    this.age = age;
		  }
	}
	
	public static void main(String[] args) {
	    
		SparkConf conf = new SparkConf().setAppName("DataFrameTest").setMaster("local");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
	    
	    // Load a text file and convert each line to a JavaBean.
	    JavaRDD<Person> people = sc.textFile("/Users/vishalshukla/Documents/workspace-sts-3.4.0.RELEASE/SparkApp/src/main/resources/people.txt").map(
	      new Function<String, Person>() {
	        public Person call(String line) throws Exception {
	          String[] parts = line.split(",");

	          Person person = new Person();
	          person.setName(parts[0]);
	          person.setAge(Integer.parseInt(parts[1].trim()));

	          return person;
	        }
	      });
	    System.out.println("Count  ppl"+people.count());
	    
	    // Apply a schema to an RDD of JavaBeans and register it as a table.
	    DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);
	    schemaPeople.registerTempTable("people");

	    // SQL can be run over RDDs that have been registered as tables.
	    DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age <= 19");
	    System.out.println("Count "+teenagers.count());
		// The results of SQL queries are DataFrames and support all the normal RDD operations.
		// The columns of a row in the result can be accessed by ordinal.
		List<String> teenagerNames = teenagers.javaRDD().map(new Function<Row, String>() {
		   public String call(Row row) {
		     return "Name: " + row.getString(0);
		   }
		}).collect();

		System.out.println("List -->"+teenagerNames.toString());
	}

}
