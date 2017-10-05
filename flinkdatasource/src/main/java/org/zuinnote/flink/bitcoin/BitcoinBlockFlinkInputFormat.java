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
 * Flink Data Source for the Bitcoin Block format
 */

package org.zuinnote.flink.bitcoin;

import java.io.IOException;

import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock;
import org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException;
import org.zuinnote.hadoop.bitcoin.format.exception.HadoopCryptoLedgerConfigurationException;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.io.CheckpointableInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.commons.logging.Log;


public class BitcoinBlockFlinkInputFormat extends AbstractBitcoinFlinkInputFormat<BitcoinBlock> implements CheckpointableInputFormat<FileInputSplit, Long> {
	


	private static final Log LOG = LogFactory.getLog(BitcoinBlockFlinkInputFormat.class.getName());
	/**
	 * 
	 */
	private static final long serialVersionUID = 4150883073922261077L;
	private boolean isEndReached;
	
	public BitcoinBlockFlinkInputFormat(int maxSizeBitcoinBlock,  String specificMagicStr,
			boolean useDirectBuffer) throws HadoopCryptoLedgerConfigurationException {
		this(maxSizeBitcoinBlock, specificMagicStr, useDirectBuffer,false);
	}
	
	public BitcoinBlockFlinkInputFormat(int maxSizeBitcoinBlock,  String specificMagicStr,
			boolean useDirectBuffer, boolean readAuxPOW) throws HadoopCryptoLedgerConfigurationException {
		super(maxSizeBitcoinBlock, specificMagicStr, useDirectBuffer);
		this.isEndReached=false;
	}
	
	@Override
	public boolean reachedEnd() throws IOException {
		return this.isEndReached;
	}
	
	/*
	 * Saves the current state of the stream
	 *  
	 *  @return current position in stream
	 *  
	 * (non-Javadoc)
	 * @see org.apache.flink.api.common.io.CheckpointableInputFormat#getCurrentState()
	 */
	
	@Override
	public Long getCurrentState() throws IOException {
		return this.stream.getPos();
	}
	
	/*
	 * Reopens the stream at a specific previously stored position and initializes the BitcoinBlockReader
	 * 
	 * @param split FileInputSplit
	 * @param state position in the stream
	 * 
	 * (non-Javadoc)
	 * @see org.apache.flink.api.common.io.CheckpointableInputFormat#reopen(org.apache.flink.core.io.InputSplit, java.io.Serializable)
	 */
	@Override
	public void reopen(FileInputSplit split, Long state) throws IOException {
		try {
			this.open(split);
		} finally {
			this.stream.seek(state);
		}
	}
	
	@Override
	public BitcoinBlock nextRecord(BitcoinBlock reuse) throws IOException {
		BitcoinBlock dataBlock=null;
		if ((this.currentSplit.getLength()<0) ||(this.stream.getPos()<=this.currentSplit.getStart()+this.currentSplit.getLength())) {
			try {
				dataBlock=this.getBbr().readBlock();
			} catch(BitcoinBlockReadException e) {
				LOG.error(e);
			}
			if (dataBlock==null) {
				this.isEndReached=true;
			}
		} else {
			this.isEndReached=true;
		}
		return dataBlock;
	}
	
}