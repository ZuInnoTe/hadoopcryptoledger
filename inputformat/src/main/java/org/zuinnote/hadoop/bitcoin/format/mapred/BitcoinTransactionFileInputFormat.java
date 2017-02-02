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
package org.zuinnote.hadoop.bitcoin.format.mapred;

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


import org.zuinnote.hadoop.bitcoin.format.common.*;

public class BitcoinTransactionFileInputFormat extends AbstractBitcoinFileInputFormat<BytesWritable,BitcoinTransaction>   {

private static final Log LOGFI = LogFactory.getLog(BitcoinTransactionFileInputFormat.class.getName());

@Override
public RecordReader<BytesWritable,BitcoinTransaction> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
	/** Create reader **/
	try {
		return new BitcoinTransactionRecordReader( (FileSplit) split,job,reporter);
	} catch (HadoopCryptoLedgerConfigurationException e) {
		// log
		LOGFI.error(e);
	} catch (BitcoinBlockReadException e) {
		// log
		LOGFI.error(e);
	}
	return null;
}



}
