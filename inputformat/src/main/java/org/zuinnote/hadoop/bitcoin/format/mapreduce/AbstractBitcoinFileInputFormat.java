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

package org.zuinnote.hadoop.bitcoin.format.mapreduce;

import org.zuinnote.hadoop.bitcoin.format.exception.HadoopCryptoLedgerConfigurationException;
import org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException;

import java.io.IOException;



import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public abstract class AbstractBitcoinFileInputFormat<K,V> extends FileInputFormat<K,V>   {
public static final String CONF_ISSPLITABLE="hadoopcryptoledeger.bitcoinblockinputformat.issplitable";
public static final boolean DEFAULT_ISSPLITABLE=false;

private static final Log LOG = LogFactory.getLog(AbstractBitcoinFileInputFormat.class.getName());

private boolean isSplitable=DEFAULT_ISSPLITABLE;


public abstract RecordReader<K,V> createRecordReader(InputSplit split, TaskAttemptContext ctx) throws IOException;

/**
*
* This method is experimental and derived from TextInputFormat. It is not necessary and not recommended to compress the blockchain files. Instead it is recommended to extract relevant data from the blockchain files once and store them in a format suitable for analytics (including compression), such as ORC or Parquet.
*
*/

@Override
  protected boolean isSplitable(JobContext context, Path file) {
    this.isSplitable=context.getConfiguration().getBoolean(this.CONF_ISSPLITABLE,this.DEFAULT_ISSPLITABLE);
    if (this.isSplitable==false) {
		return false;
    }   
   final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    if (null == codec) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }




}
