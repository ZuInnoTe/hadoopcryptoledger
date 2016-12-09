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


import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.io.BytesWritable; 


import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.zuinnote.hadoop.bitcoin.format.common.*;

public class BitcoinTransactionRecordReader extends AbstractBitcoinRecordReader<BytesWritable, BitcoinTransaction> {
private static final Log LOG = LogFactory.getLog(BitcoinBlockRecordReader.class.getName());

private int currentTransactionCounterInBlock=0;
private BitcoinBlock currentBitcoinBlock;



public BitcoinTransactionRecordReader(FileSplit split,JobConf job, Reporter reporter) throws IOException,HadoopCryptoLedgerConfigurationException,BitcoinBlockReadException {
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
public BitcoinTransaction createValue() {
	return new BitcoinTransaction();
}


/**
*
* Read a next block. 
*
* @param key is a 68 byte array (hashMerkleRoot, prevHashBlock, transActionCounter)
* @param value is a deserialized Java object of class BitcoinBlock
*
* @return true if next block is available, false if not
*/
@Override
public boolean next(BytesWritable key, BitcoinTransaction value) throws IOException {
	// read all the blocks, if necessary a block overlapping a split
	while(getFilePosition()<=getEnd()) { // did we already went beyond the split (remote) or do we have no further data left?
		if ((currentBitcoinBlock==null) || (currentBitcoinBlock.getTransactions().size()==currentTransactionCounterInBlock)){
			try {
				currentBitcoinBlock=getBbr().readBlock();
				currentTransactionCounterInBlock=0;
			} catch (BitcoinBlockReadException e) {
				// log
				LOG.error(e);
			}
		}

		if (currentBitcoinBlock==null) {
			return false;
		}
		BitcoinTransaction currentTransaction=currentBitcoinBlock.getTransactions().get(currentTransactionCounterInBlock);
		// the unique identifier that is linked in other transaction is usually its hash
		byte[] newKey = new byte[0];
		try {
			newKey=BitcoinUtil.getTransactionHash(currentTransaction);
		} catch (NoSuchAlgorithmException nsae) {
			LOG.error("Cannot calculate transaction hash. Algorithm not available. Exception: "+nsae.toString());
		}
		key.set(newKey,0,newKey.length);
		value.set(currentTransaction);
		currentTransactionCounterInBlock++;
		return true;
	}
	return false;
}



}
