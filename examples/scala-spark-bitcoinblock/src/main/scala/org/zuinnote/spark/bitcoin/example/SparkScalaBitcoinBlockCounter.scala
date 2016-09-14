

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


