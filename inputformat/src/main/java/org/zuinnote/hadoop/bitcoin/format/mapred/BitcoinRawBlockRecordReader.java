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
import java.io.InputStream;

import java.nio.ByteBuffer;

import org.apache.hadoop.io.BytesWritable; 

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;




public class BitcoinRawBlockRecordReader  extends AbstractBitcoinRecordReader<BytesWritable, BytesWritable> {
private static final Log LOG = LogFactory.getLog(BitcoinRawBlockRecordReader.class.getName());


public BitcoinRawBlockRecordReader(FileSplit split,JobConf job, Reporter reporter) throws IOException,HadoopCryptoLedgerConfigurationException,BitcoinBlockReadException {
	super(split,job,reporter);
}


	

/**
*
* Create an empty key
*
* @return key
*/
public BytesWritable createKey() {
	return new BytesWritable();
}

/**
*
* Create an empty value
*
* @return value
*/
public BytesWritable createValue() {
	return new BytesWritable();
}



/**
*
* Read a next block. 
*
* @param key is a 64 byte array (hashMerkleRoot and prevHashBlock)
* @param value is a deserialized Java object of class BitcoinBlock
*
* @return true if next block is available, false if not
*/
public boolean next(BytesWritable key, BytesWritable value) throws IOException {
	// read all the blocks, if necessary a block overlapping a split
	while(getFilePosition()<=getEnd()) { // did we already went beyond the split (remote) or do we have no further data left?
		ByteBuffer dataBlock=null;
		try {
			dataBlock=getBbr().readRawBlock();
		} catch (BitcoinBlockReadException e) {
			// log
			LOG.error(e);
		}	
		if (dataBlock==null) return false;
		byte newKey[]=getBbr().getKeyFromRawBlock(dataBlock);
		key.set(newKey,0,newKey.length);
		byte[] dataBlockArray=null;
		if (dataBlock.hasArray()==true) {
			dataBlockArray=dataBlock.array();
		} else {
			dataBlockArray=new byte[dataBlock.capacity()];
			dataBlock.get(dataBlockArray);
		}
		value.set(dataBlockArray,0,dataBlockArray.length);
		return true;
	}
	return false;
}


}
