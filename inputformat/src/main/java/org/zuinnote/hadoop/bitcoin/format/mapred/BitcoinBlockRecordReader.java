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

import org.apache.hadoop.io.BytesWritable; 

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.bitcoin.format.common.*;

/**
* Reads records as blocks of the bitcoin blockchain. Note that it can be tricky to find the start of a block in a split. The BitcoinBlockReader provides a method (seekBlockStart) for this.
*
*/

public class BitcoinBlockRecordReader extends AbstractBitcoinRecordReader<BytesWritable, BitcoinBlock>  {
private static final Log LOG = LogFactory.getLog(BitcoinBlockRecordReader.class.getName());


public BitcoinBlockRecordReader(FileSplit split,JobConf job, Reporter reporter) throws IOException,HadoopCryptoLedgerConfigurationException,BitcoinBlockReadException {
	super(split,job,reporter);
}

/**
*
* Create an empty key
*
* @return key
*/
@Override
public BytesWritable createKey() {
	return new BytesWritable();
}

/**
*
* Create an empty value
*
* @return value
*/
@Override
public BitcoinBlock createValue() {
	return new BitcoinBlock();
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
@Override
public boolean next(BytesWritable key, BitcoinBlock value) throws IOException {
	// read all the blocks, if necessary a block overlapping a split
	while(getFilePosition()<=getEnd()) { // did we already went beyond the split (remote) or do we have no further data left?
		BitcoinBlock dataBlock=null;
		try {
			dataBlock=getBbr().readBlock();
			
		} catch (BitcoinBlockReadException e) {
			// log
			LOG.error(e);
		}	
		if (dataBlock==null) { 
			return false;
		}
		byte[] hashMerkleRoot=dataBlock.getHashMerkleRoot();
		byte[] hashPrevBlock=dataBlock.getHashPrevBlock();
		byte[] newKey=new byte[hashMerkleRoot.length+hashPrevBlock.length];
		for (int i=0;i<hashMerkleRoot.length;i++) {
			newKey[i]=hashMerkleRoot[i];
		}
		for (int j=0;j<hashPrevBlock.length;j++) {
			newKey[j+hashMerkleRoot.length]=hashPrevBlock[j];
		}
		key.set(newKey,0,newKey.length);
		value.set(dataBlock);
		return true;
	}
	return false;
}


}
