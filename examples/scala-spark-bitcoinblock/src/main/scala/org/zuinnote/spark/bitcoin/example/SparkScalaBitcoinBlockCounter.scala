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
package org.zuinnote.spark.bitcoin.example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.conf._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred._
import org.apache.hadoop.io._

import org.zuinnote.hadoop.bitcoin.format._
   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

object SparkScalaBitcoinBlockCounter {
   def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Spark-Scala BitcoinBlock Analytics (hadoopcryptoledger)")
	val sc=new SparkContext(conf)
	val hadoopConf = new JobConf();
	FileInputFormat.addInputPath(hadoopConf, new Path(args(0)));
	 hadoopConf.set("hadoopcryptoledger.bitcoinblockinputformat.filter.magic","F9BEB4D9");
	val bitcoinBlocksRDD = sc.hadoopRDD(hadoopConf, classOf[BitcoinBlockFileInputFormat], classOf[BytesWritable], classOf[BitcoinBlock], 2);
	// extract the no transactions / block (map)
   	val noOfTransactionPair = bitcoinBlocksRDD.map(hadoopKeyValueTuple => ("No of transactions: ",hadoopKeyValueTuple._2.getTransactions().size()));
	// reduce total count
	val totalCount = noOfTransactionPair.reduceByKey ((c,d) => c+d);

    	// write results to HDFS
	totalCount.repartition(1).saveAsTextFile(args(1));
      }
    }


