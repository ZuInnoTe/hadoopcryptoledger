/**
* Copyright 2017 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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
 * Simple Driver for a map reduce job counting the number of transactons in a given blocks from the specified files containing Bitcoin blockchain data
 */
package org.zuinnote.hadoop.ethereum.example.driver;

import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import org.zuinnote.hadoop.ethereum.format.mapreduce.EthereumBlockFileInputFormat;
import org.zuinnote.hadoop.ethereum.example.tasks.EthereumBlockMap;
import org.zuinnote.hadoop.ethereum.example.tasks.EthereumBlockReducer;
   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

public class EthereumBlockCounterDriver extends Configured implements Tool {

 public EthereumBlockCounterDriver() {
	// nothing needed here
 }      
        
 public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(),"example-hadoop-ethereum-transactioncounter-job");
   	job.setJarByClass(EthereumBlockCounterDriver.class);
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(IntWritable.class);
    	job.setOutputKeyClass(Text.class);
   	job.setOutputValueClass(LongWritable.class);
        
    	job.setMapperClass(EthereumBlockMap.class);
    	job.setReducerClass(EthereumBlockReducer.class);
        
    	job.setInputFormatClass(EthereumBlockFileInputFormat.class);
    	job.setOutputFormatClass(TextOutputFormat.class);
     	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
         return job.waitForCompletion(true)?0:1;
       }
       
       public static void main(String[] args) throws Exception {
	 Configuration conf = new Configuration();
   	 /** Set as an example some of the options to configure the Ethereum fileformat **/
    	 /** Find here all configuration options: https://github.com/ZuInnoTe/hadoopcryptoledger/wiki/Hadoop-File-Format **/
    	conf.set("hadoopcryptoledeger.ethereumblockinputformat.usedirectbuffer","false");
         // Let ToolRunner handle generic command-line options 
         int res = ToolRunner.run(conf, new EthereumBlockCounterDriver(), args); 
         System.exit(res);
       }
        
}
