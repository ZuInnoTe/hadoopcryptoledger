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

/**
 * Flink Data Source for the Bitcoin Raw Block format
 */
package org.zuinnote.flink.bitcoin;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException;
import org.zuinnote.hadoop.bitcoin.format.exception.HadoopCryptoLedgerConfigurationException;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.commons.logging.Log;


public class BitcoinRawBlockFlinkInputFormat extends AbstractBitcoinFlinkInputFormat<BytesWritable> {
	


	private static final Log LOG = LogFactory.getLog(BitcoinRawBlockFlinkInputFormat.class.getName());
	/**
	 * 
	 */
	private static final long serialVersionUID = 4150883073922261077L;
	private boolean isEndReached;
	
	public BitcoinRawBlockFlinkInputFormat(int maxSizeBitcoinBlock, int bufferSize, String specificMagicStr,
			boolean useDirectBuffer) throws HadoopCryptoLedgerConfigurationException {
		super(maxSizeBitcoinBlock, bufferSize, specificMagicStr, useDirectBuffer);
		this.isEndReached=false;
	}
	
	@Override
	public boolean reachedEnd() throws IOException {
		return this.isEndReached;
	}

	@Override
	public BytesWritable nextRecord(BytesWritable reuse) throws IOException {
		ByteBuffer dataBlock=null;
		if (this.stream.getPos()<=this.currentSplit.getStart()+this.currentSplit.getLength()) {			
			try {
				dataBlock=this.getBbr().readRawBlock();
			} catch(BitcoinBlockReadException e) {
				LOG.error(e);
			}
			if (dataBlock==null) {
				this.isEndReached=true;
			} else {
				byte[] dataBlockArray;
				if (dataBlock.hasArray()) {
					dataBlockArray=dataBlock.array();
				} else {
					dataBlockArray=new byte[dataBlock.capacity()];
					dataBlock.get(dataBlockArray);
				}
				reuse.set(dataBlockArray,0,dataBlockArray.length);
				return reuse;
			}
		}
		else {
			this.isEndReached=true;
		}
		return null;
	}
	
}