package org.AnomalyIndex.Anomaly;

import java.util.Arrays;
import java.util.List;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.types.DataTypes;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;

import org.apache.spark.sql.Row;

import org.apache.spark.sql.RowFactory;

public class Anomaly {
    public static void main(String[] args) throws Exception {
    
//  Create a Java Spark Context.
    SparkConf conf = new SparkConf().setAppName("AnomalyIndex");
	
//  SparkConf conf = new SparkConf().setAppName("AnomalyIndex").setMaster("local[4]"); // Here 4 is number of cores. you can change it accordig to your machine.
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

//  Load our input data. It will create pair RDD with filename as key and File content as its value. <filename,filecontent>
    //String InputFilePath = args[0];
    String InputFilePath = args[0];
	String OutputFilePath = args[1];
    JavaPairRDD<String, String> filesRDD = sc.wholeTextFiles(InputFilePath);
//  System.out.println(filesRDD.toDebugString());

//  Convert file content text into array of words. It will keep key as it is.
    JavaPairRDD<String, String[]> wordsRDD = filesRDD.mapValues(
        new Function<String,String[]>(){
           public String[] call(String value) throws Exception {
              return value.split(" ");
           }
        }
    );
//  System.out.println(wordsRDD.toDebugString());
    
// Convert PairRDD into RowRDD. RowRDD is required to create dataframes.
    JavaRDD<Row> rowRDD = wordsRDD.map(
        new Function<Tuple2<String, String[]>, Row>() {
            public Row call(Tuple2<String, String[]> record) throws Exception {
                Tuple2<String, String[]> fields = new Tuple2(record._1,record._2);
                return RowFactory.create(fields._1, fields._2);
             }
        }
    );

//  System.out.println(rowRDD.toDebugString());

//  Define schema for our dataframe.
    List<StructField> fields = Arrays.asList(
        DataTypes.createStructField("FileName", DataTypes.StringType, true),
        DataTypes.createStructField("FileContent", DataTypes.createArrayType(DataTypes.StringType), true)
    );
    StructType schema = DataTypes.createStructType(fields);
    DataFrame df = sqlContext.createDataFrame(rowRDD, schema);
//  df.printSchema();


//  As of now we have filename and whole file content in one row.
//  So now we create new RDD which contains file name and a single word in a row. So it will create new row for each word.
    
    DataFrame explodedDF1 = df.select(df.col("FileName"), org.apache.spark.sql.functions.explode(df.col("FileContent")).as("Word"));

    DataFrame explodedDF = explodedDF1.withColumn("Words", org.apache.spark.sql.functions.trim(explodedDF1.col("Word"))).select("FileName","Words");
    
//  Now we have set of words with its file name.

//  explodedDF.printSchema();
//  explodedDF.show(25);

//  Calculate How many times a word appears in a particular file. Let's call it fw(d)
    DataFrame fileWordCountDF = explodedDF.groupBy(explodedDF.col("FileName"), explodedDF.col("Words")).
       count().withColumnRenamed("count", "FileWordCount");
  
//  Calculate total how many times a word appears in all files. Let's call it fw(D)
    DataFrame wordCountDF = explodedDF.groupBy(explodedDF.col("Words")).count().withColumnRenamed("count", "WordCount");
 
//  Calculate number of words in a file. Let's call it m(d).
    DataFrame fileWordsTotalDF = explodedDF.groupBy(explodedDF.col("FileName")).count().withColumnRenamed("count", "FileWordsTotal");

//  Calculate total number of words in all files. Let's call it m.
    long wodsTotalDF = explodedDF.count();  
 
//  This value is going to be used by all Executors. So we broadcast it to all Executors.
    Broadcast<Long> wordsTotalDFBc = sc.broadcast(wodsTotalDF);

//  Join two DataFrames so new Dataframe will contain FileName, Word, fw(d) and m(d).

    DataFrame joinedDF = fileWordCountDF.join(fileWordsTotalDF, "FileName");
//  Join another DataFrame so that new DataFrames will have all data we need. FIleName, Word, fw(d), fw(D) and m(d).
    DataFrame dataSet = joinedDF.join(wordCountDF, "Words"); 

//  As this DataFrame contains all information, it will be good if we cache it.
    dataSet.cache();
    
//  Calculate fw(D) / m .

    DataFrame dataSet1 = dataSet.withColumn("Ratio1", dataSet.col("WordCount").divide(wordsTotalDFBc.value()));

//  Calculate fw(d) / m(d).
    
    DataFrame dataSet2 = dataSet1.withColumn("Ratio2", dataSet1.col("FileWordCount").divide(dataSet1.col("FileWordsTotal")));
	
//  Calculate (fw(d)/m(d)) / (fw(D)/m).
	
    DataFrame dataSet3 = dataSet2.withColumn("Ratio", dataSet2.col("Ratio2").divide(dataSet2.col("Ratio1")));
	
//  Calculate log of Ratio.
    DataFrame dataSet4 = dataSet3.withColumn("logRatio", org.apache.spark.sql.functions.log(dataSet3.col("Ratio")));
	
	
    DataFrame dataSet5 = dataSet4.withColumn("AnomalyIndex", dataSet4.col("Ratio2").multiply(dataSet4.col("logRatio")));
	
//  Sum of AnomalyIndex value all words	per file.
    DataFrame anomalyIndex = dataSet5.select("FileName","Words","AnomalyIndex").groupBy("FileName").sum("AnomalyIndex").withColumnRenamed("sum(AnomalyIndex)", "AnomalyIndex");

//  To save Anomaly Index of all files into Outputfile.  
//  anomalyIndex.toJavaRDD().saveAsTextFile(OutputFilePath);
    
//  Find Top 5 Files with Maximum Anomaly Index.
    DataFrame top5 = anomalyIndex.sort(org.apache.spark.sql.functions.desc("AnomalyIndex")).limit(5);

//This will display Top 5 on Console
    top5.show();
//This will save top 5 to file
    top5.javaRDD().saveAsTextFile(OutputFilePath);
   }
}​
