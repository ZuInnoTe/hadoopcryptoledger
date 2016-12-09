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
 * Simple Mapper for counting the number of Bitcoin transactions in a file on HDFS
 */
package org.zuinnote.hadoop.bitcoin.example.tasks;

/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/
import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.zuinnote.hadoop.bitcoin.format.common.*;

import org.zuinnote.hadoop.bitcoin.format.mapreduce.*;


import java.util.*;

public  class BitcoinBlockMap  extends Mapper<BytesWritable, BitcoinBlock, Text, IntWritable> {
private final static Text defaultKey = new Text("Transaction Count:");


@Override
public void setup(Context context) throws IOException, InterruptedException {
}

@Override
public void map(BytesWritable key, BitcoinBlock value, Context context) throws IOException, InterruptedException {
	// get the number of transactions
	context.write(defaultKey, new IntWritable(value.getTransactions().size()));
}

@Override
public void cleanup(Context context) {
}	    

}
	 
