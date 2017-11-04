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
package org.zuinnote.hadoop.ethereum.format.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;




/**

 *
 */
public abstract class AbstractEthereumFileInputFormat <K,V> extends FileInputFormat<K,V> implements JobConfigurable{
	
	private CompressionCodecFactory compressionCodecs = null;
	
	@Override
	public void configure(JobConf job) {
		 this.compressionCodecs = new CompressionCodecFactory(job);
		
	}

	/**
	*
	* Ethereum blockchain data is not splitable by definition. However, you can export the blockchains in files of a certain size (recommended: HDFS blocksize) to benefit from parallelism. It is not necessary and not recommended to compress the blockchain files. Instead it is recommended to extract relevant data from the blockchain files once and store them in a format suitable for analytics (including compression), such as ORC or Parquet.
	*
	*/
	@Override
	  protected boolean isSplitable(FileSystem fs, Path file) {
			return false;

	  }



}
