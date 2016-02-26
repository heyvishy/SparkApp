package org.sparkApp.df;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class WordCountDF {

	public static class Word implements Serializable{
		
		private String word;
		private int count;
		
		public String getWord() {
			return word;
		}
		public void setWord(String word) {
			this.word = word;
		}
		public int getCount() {
			return count;
		}
		public void setCount(int count) {
			this.count = count;
		}
	}
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("WordCountDF").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		JavaRDD<String> textFile = sc.textFile("src/main/resources/testfile.txt");
		JavaRDD<String> wordsRDD = textFile.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String lines) throws Exception {
				return Arrays.asList(lines.split(" "));
			}
		});
		
		// Load a text file and convert each line to a JavaBean.
		JavaRDD<Word> words = wordsRDD.map(
		  new Function<String, Word>() {
		    public Word call(String inputWord) throws Exception {
		      Word word = new Word();
		      word.setWord(inputWord);
		      word.setCount(1);
		      return word;
		    }
		  });
		
		// Apply a schema to an RDD of JavaBeans and register it as a table.
		DataFrame schemaWord = sqlContext.createDataFrame(words, Word.class);
		schemaWord.registerTempTable("words");

		// SQL can be run over RDDs that have been registered as tables.
		DataFrame allWords = sqlContext.sql("SELECT count(*) AS wordCount,word FROM words GROUP BY word");

		// The results of SQL queries are DataFrames and support all the normal RDD operations.
		// The columns of a row in the result can be accessed by ordinal.
		allWords.show(100);
		
	}
	

}
