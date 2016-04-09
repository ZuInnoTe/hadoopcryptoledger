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
 * Simple Mapper for counting the number Bitcoin blocks in a file on HDFS
 */
package org.zuinnote.hadoop.bitcoin.example.tasks;

/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/
import java.io.IOException;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.zuinnote.hadoop.bitcoin.format.*;


import java.util.*;

	 public  class BitcoinBlockMap  extends MapReduceBase implements Mapper<BytesWritable, BitcoinBlock, Text, IntWritable> {
	    private final static Text defaultKey = new Text("Transaction Count:");
	    public void map(BytesWritable key, BitcoinBlock value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    	// get the number of transactions
	    	 output.collect(defaultKey, new IntWritable(value.getTransactions().length));
	    	 }
	    
	    }
	 
