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
 * Simple Driver for a Spark 2 job counting the number of transactons in a given block from the specified files containing Bitcoin blockchain data
 */
package org.zuinnote.spark2.bitcoin.example;

import java.util.*;
        
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


import scala.Tuple2;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;

import org.zuinnote.hadoop.bitcoin.format.common.*;

import org.zuinnote.hadoop.bitcoin.format.mapreduce.*;
   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

public class Spark2BitcoinBlockCounter  {

public Spark2BitcoinBlockCounter() {
	super();
}
        
        
 public static void main(String[] args) throws Exception {
    SparkConf conf = new SparkConf().setAppName("Spark2 BitcoinBlock Analytics (hadoopcryptoledger)");
    JavaSparkContext sc = new JavaSparkContext(conf); 
    // create Hadoop Configuration
    Configuration hadoopConf= new Configuration();
      /** Set as an example some of the options to configure the Bitcoin fileformat **/
     /** Find here all configuration options: https://github.com/ZuInnoTe/hadoopcryptoledger/wiki/Hadoop-File-Format **/
    hadoopConf.set("hadoopcryptoledger.bitcoinblockinputformat.filter.magic","F9BEB4D9");
    
    // read bitcoin data from HDFS
    JavaPairRDD<BytesWritable, BitcoinBlock> bitcoinBlocksRDD = sc.newAPIHadoopFile(args[0], BitcoinBlockFileInputFormat.class, BytesWritable.class, BitcoinBlock.class,hadoopConf);
    // extract the no transactions / block (map)
    JavaPairRDD<String, Long> noOfTransactionPair = bitcoinBlocksRDD.mapToPair(new PairFunction<Tuple2<BytesWritable,BitcoinBlock>, String, Long>() {
	@Override
	public Tuple2<String, Long> call(Tuple2<BytesWritable,BitcoinBlock> tupleBlock) {
		return mapNoOfTransaction(tupleBlock._2());
	}
    });
   // combine the results from all blocks
   JavaPairRDD<String, Long> totalCount = noOfTransactionPair.reduceByKey(new Function2<Long, Long, Long>() {
	@Override	
	public Long call(Long a, Long b) { 
		return reduceSumUpTransactions(a,b);
	}
   });
    // write results to HDFS
    totalCount.repartition(1).saveAsTextFile(args[1]);
    sc.close();
}



    /**
     * Maps the number of transactions of a block to a tuple
     *
     * @param block Bitcoinblock
     *
     * @return Tuple containing the String "No of transactions. " and the number of transactions as long 
     *
    **/
  public static Tuple2<String,Long> mapNoOfTransaction(BitcoinBlock block) {
	return new Tuple2<String, Long>("No of transactions: ",(long)(block.getTransactions().size())); 
  }

   /**
     * Sums up the transaction count within a reduce step
     * 
     * @param a transaction count
     * @param b transaction count
     *
     * @return sum of a and b
     *
    **/
  public static Long reduceSumUpTransactions(Long a, Long b) {
	return a+b;
  }
        
}
