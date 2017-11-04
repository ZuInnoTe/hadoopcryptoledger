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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.zuinnote.hadoop.ethereum.format.common.EthereumBlock;
import org.zuinnote.hadoop.ethereum.format.exception.EthereumBlockReadException;

/**

 *
 */
public class EthereumBlockRecordReader extends AbstractEthereumRecordReader<BytesWritable,EthereumBlock> {
	private static final Log LOG = LogFactory.getLog(EthereumBlockRecordReader.class.getName());
	public EthereumBlockRecordReader(FileSplit split, JobConf job, Reporter reporter) throws IOException {
		super(split,job,reporter);
	}
	

/**
*
* Read a next block. 
*
* @param key is a 32 byte array (parentHash)
* @param value is a deserialized Java object of class EthereumBlock
*
* @return true if next block is available, false if not
*/
@Override
public boolean next(BytesWritable key, EthereumBlock value) throws IOException {
	// read all the blocks, if necessary a block overlapping a split
	while(getFilePosition()<=getEnd()) { // did we already went beyond the split (remote) or do we have no further data left?
		EthereumBlock dataBlock=null;
		try {
			dataBlock=getEbr().readBlock();
		} catch (EthereumBlockReadException e) {
			LOG.error(e);
			throw new RuntimeException(e.toString());
		}
		if (dataBlock==null) { 
			return false;
		}
		byte[] newKey=dataBlock.getEthereumBlockHeader().getParentHash();
		key.set(newKey,0,newKey.length);
		value.set(dataBlock);
		return true;
	}
	return false;
}


	@Override
	public BytesWritable createKey() {
		return new BytesWritable();
	}

	@Override
	public EthereumBlock createValue() {
		return new EthereumBlock();
	}

}
