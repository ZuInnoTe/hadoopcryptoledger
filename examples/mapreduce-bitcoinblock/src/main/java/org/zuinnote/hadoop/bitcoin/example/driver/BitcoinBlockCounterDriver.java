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
 * Simple Driver for a map reduce job counting the number of transactons in a given blocks from the specified files containing Bitcoin blockchain data
 */
package org.zuinnote.hadoop.bitcoin.example.driver;

import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.zuinnote.hadoop.bitcoin.example.tasks.BitcoinBlockMap;
import org.zuinnote.hadoop.bitcoin.example.tasks.BitcoinBlockReducer;
   
import org.zuinnote.hadoop.bitcoin.format.*;
   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

public class BitcoinBlockCounterDriver  {

       
        
 public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(BitcoinBlockCounterDriver.class);
    conf.setJobName("example-hadoop-bitcoin-transactioncounter-job");
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(IntWritable.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LongWritable.class);
        
    conf.setMapperClass(BitcoinBlockMap.class);
    conf.setReducerClass(BitcoinBlockReducer.class);
        
    conf.setInputFormat(BitcoinBlockFileInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
    JobClient.runJob(conf);
 }
        
}
