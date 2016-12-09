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
 * Simple Driver for a map reduce job counting the number of transacton inputs in a given blocks from the specified files containing Bitcoin blockchain data
 */
package org.zuinnote.hadoop.bitcoin.example.driver;

import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.zuinnote.hadoop.bitcoin.example.tasks.BitcoinTransactionMap;
import org.zuinnote.hadoop.bitcoin.example.tasks.BitcoinTransactionReducer;
   
import org.zuinnote.hadoop.bitcoin.format.common.*;

import org.zuinnote.hadoop.bitcoin.format.mapreduce.*;

/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

public class BitcoinTransactionCounterDriver  {

 private BitcoinTransactionCounterDriver() {
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf,"example-hadoop-bitcoin-transactioncounter-job");
    job.setJarByClass(BitcoinTransactionCounterDriver.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
        
    job.setMapperClass(BitcoinTransactionMap.class);
    job.setReducerClass(BitcoinTransactionReducer.class);
        
    job.setInputFormatClass(BitcoinTransactionFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    /** Set as an example some of the options to configure the Bitcoin fileformat **/
     /** Find here all configuration options: https://github.com/ZuInnoTe/hadoopcryptoledger/wiki/Hadoop-File-Format **/
    conf.set("hadoopcryptoledger.bitcoinblockinputformat.filter.magic","F9BEB4D9");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    System.exit(job.waitForCompletion(true)?0:1);

 }
        
}
