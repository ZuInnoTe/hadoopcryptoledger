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
package org.zuinnote.spark.ethereum.example

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.conf._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred._
import org.apache.hadoop.io._

import org.apache.spark.sql.functions._

   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

/**
* Demonstrate the HadoopCryptoLedger Spark Datasource API by loading the Ethereum Blockchain
*
*
*/

object SparkScalaEthereumBlockDataSource {
   def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Spark-Scala EthereumBlock Analytics (hadoopcryptoledger) - Datasource API")
	val sc=new SparkContext(conf)
	val sqlContext = new SQLContext(sc)
	sumTransactionOutputsJob(sqlContext,args(0),args(1))
	sc.stop()
      }

	def sumTransactionOutputsJob(sqlContext: SQLContext, inputFile: String, outputFile: String): Unit = {
	val df = sqlContext.read
    	.format("org.zuinnote.spark.ethereum.block")
    	.option("enrich", "false") // set an option
    	// other options maxBlockSize, useDirectBuffer, enrich (to load also transactionHash and sendAddress of transaction)
    	.load(inputFile)
	// print schema
	df.printSchema
	// extract all the outputs of the transactions and summarize them
	val transactionsDF=df.select(explode(df("ethereumTransactions")).alias("ethereumTransactions"))
	val summarizedOutputs=transactionsDF.agg(sum(transactionsDF("ethereumTransactions.value")))
	// store the result on HDFS
	summarizedOutputs.rdd.saveAsTextFile(outputFile)
	}
    }


