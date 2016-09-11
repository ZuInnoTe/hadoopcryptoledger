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

package org.zuinnote.hadoop.bitcoin.format;

import org.zuinnote.hadoop.bitcoin.format.exception.HadoopCryptoLedgerConfigurationException;
import org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException;

import java.io.IOException;



import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public class BitcoinBlockFileInputFormat extends FileInputFormat<BytesWritable,BitcoinBlock> implements JobConfigurable  {

private static final Log LOG = LogFactory.getLog(BitcoinBlockFileInputFormat.class.getName());
private static final String CONF_ISSPLITABLE="hadoopcryptoledeger.bitcoinblockinputformat.issplitable";
private static final boolean DEFAULT_ISSPLITABLE=false;
private boolean isSplitable=DEFAULT_ISSPLITABLE;
private CompressionCodecFactory compressionCodecs = null;


public RecordReader<BytesWritable,BitcoinBlock> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {	
	/** Create reader **/
	try {
		return new BitcoinBlockRecordReader( (FileSplit) split,job,reporter);
	} catch (HadoopCryptoLedgerConfigurationException e) {
		// log
		LOG.error(e);
	} catch (BitcoinBlockReadException e) {
		// log
		LOG.error(e);
	}
	return null;
}



public void configure(JobConf conf) {
    this.compressionCodecs = new CompressionCodecFactory(conf);
    this.isSplitable=conf.getBoolean(this.CONF_ISSPLITABLE,this.DEFAULT_ISSPLITABLE);
}

/**
*
* This method is experimental and derived from TextInputFormat. It is not necessary and not recommended to compress the blockchain files. Instead it is recommended to extract relevant data from the blockchain files once and store them in a format suitable for analytics (including compression), such as ORC or Parquet.
*
*/

  protected boolean isSplitable(FileSystem fs, Path file) {
    if (this.isSplitable==false) return false;
    final CompressionCodec codec = compressionCodecs.getCodec(file);
    if (null == codec) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;

  }






}
