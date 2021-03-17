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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;

import org.zuinnote.hadoop.ethereum.format.common.EthereumBlock;
import org.zuinnote.hadoop.ethereum.format.common.EthereumBlockWritable;
import org.zuinnote.hadoop.ethereum.format.exception.EthereumBlockReadException;
import org.zuinnote.hadoop.ethereum.format.mapreduce.AbstractEthereumRecordReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 *
 */
public class EthereumBlockRecordReader extends AbstractEthereumRecordReader<BytesWritable,EthereumBlockWritable>{


	private static final Log LOG = LogFactory.getLog(EthereumBlockRecordReader.class.getName());
	private BytesWritable currentKey=new BytesWritable();
	private EthereumBlockWritable currentValue=new EthereumBlockWritable();

	public EthereumBlockRecordReader(Configuration conf) {
		super(conf);
	}


	/**
	*
	* Read a next block.
	*
	*
	* @return true if next block is available, false if not
	*/
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// read all the blocks, if necessary a block overlapping a split
		while(getFilePosition()<=getEnd()) { // did we already went beyond the split (remote) or do we have no further data left?
			EthereumBlock dataBlock=null;
			try {
				dataBlock=getEbr().readBlock();
			} catch (EthereumBlockReadException e) {
				LOG.error(e);
				throw new InterruptedException(e.toString());
			}
			if (dataBlock==null) {
				return false;
			}

			byte[] newKey=dataBlock.getEthereumBlockHeader().getParentHash();
			this.currentKey.set(newKey,0,newKey.length);
			this.currentValue.set(dataBlock);
			return true;
		}
	return false;
	}

	/**
	*
	*  get current key after calling next()
	*
	* @return key is a 32byte array (parentHash)
	*/

	@Override
	public BytesWritable getCurrentKey() throws IOException, InterruptedException {
		return this.currentKey;
	}

	/**
	*
	*  get current value after calling next()
	*
	* @return is a deserialized Java object of class EthereumBlock
	*/

	@Override
	public EthereumBlockWritable getCurrentValue() throws IOException, InterruptedException {
		return this.currentValue;
	}

}
