package com.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;


public class SimpleApp 
{
    public static void main( String[] args )
    {
	    String logFile = "data_file.txt";
	    
	    //Set this to resolve - java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
	    //System.setProperty("hadoop.home.dir", "full path to the bin folder with winutils");
	    
	    SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master", "local").getOrCreate();
	    //SparkConf conf = new SparkConf();
	    
	    //conf.set("spark.testing.memory", "2147480000");
	    
	    
	    Dataset<String> logData = spark.read().textFile(logFile).cache();

	    long numAs = logData.filter(s -> s.contains("a")).count();
	    long numBs = logData.filter(s -> s.contains("b")).count();

	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	    
	    

	    spark.stop();
    }
}
