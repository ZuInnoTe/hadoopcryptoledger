/**
* Copyright 2016 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
**/

/**
 * Simple Driver for a Spark job counting the number of transactons in a given block from the specified files containing Bitcoin blockchain data. It uses the DataFrame API instead of RDDs.
 */
package org.zuinnote.spark.bitcoin.example;

import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


import org.apache.hadoop.conf.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;

import org.zuinnote.hadoop.bitcoin.format.*;
   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

public class SparkDataFrameBitcoinBlockCounter  {

       
        
 public static void main(String[] args) throws Exception {
    SparkConf conf = new SparkConf().setAppName("Spark BitcoinBlock Analytics (hadoopcryptoledger)");
    JavaSparkContext sc = new JavaSparkContext(conf); 
    // create SQL Context for data frame
    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
	// activate compression for higher performance
    sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed","true");
	// activate tungsten
    sqlContext.setConf("spark.sql.tungsten.enabled","true");
    // create Hadoop Configuration
    JobConf hadoopConf= new JobConf();
    FileInputFormat.addInputPath(hadoopConf, new Path(args[0]));
      /** Set as an example some of the options to configure the Bitcoin fileformat **/
     /** Find here all configuration options: https://github.com/ZuInnoTe/hadoopcryptoledger/wiki/Hadoop-File-Format **/
    hadoopConf.set("hadoopcryptoledger.bitcoinblockinputformat.filter.magic","F9BEB4D9");
    // read bitcoin data from HDFS
    JavaPairRDD<BytesWritable, BitcoinBlock> bitcoinBlocksPairRDD = sc.hadoopRDD(hadoopConf, BitcoinBlockFileInputFormat.class, BytesWritable.class, BitcoinBlock.class, 2);
    // only use the BitcoinBlock and not the key
    JavaRDD<BitcoinBlock> bitcoinBlockRDD = bitcoinBlocksPairRDD.values();
    // convert it to DataFrame
     Dataset<Row> bitcoinBlockDF = sqlContext.createDataFrame(bitcoinBlockRDD, BitcoinBlock.class);
    bitcoinBlockDF.registerTempTable("BitcoinBlockChain");
    // cache it if you do a lot of iterative queries
     sqlContext.cacheTable("BitcoinBlockChain");
    Dataset<Row> allBlockCountDF = sqlContext.sql("SELECT count(*) FROM BitcoinBlockChain");
  
    // write results to HDFS
    allBlockCountDF.write().save(args[1]);
 }
        
}
