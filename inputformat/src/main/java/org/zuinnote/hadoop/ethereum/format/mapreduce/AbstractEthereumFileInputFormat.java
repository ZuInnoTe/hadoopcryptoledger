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
package org.zuinnote.hadoop.ethereum.format.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *
 */
public abstract class AbstractEthereumFileInputFormat<K,V> extends FileInputFormat<K,V> {

	@Override
	public abstract RecordReader<K,V> createRecordReader(InputSplit split, TaskAttemptContext ctx) throws IOException;

	
	/***
	 * The Ethereum format is not splitable due to its RLP encoding which makes it difficult to find the start/end of a RLP encoded item. Instead it is recommended to export blocks to files of the size of a HDFS block.
	 * 
	 * 
	 */
	@Override
	  protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

}
