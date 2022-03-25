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
 * Flink Data Source for the Bitcoin Transaction format
 */

package org.zuinnote.flink.bitcoin;


import java.io.IOException;

import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinTransaction;
import org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException;
import org.zuinnote.hadoop.bitcoin.format.exception.HadoopCryptoLedgerConfigurationException;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.io.CheckpointableInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.commons.logging.Log;


public class BitcoinTransactionFlinkInputFormat extends AbstractBitcoinFlinkInputFormat<BitcoinTransaction> implements CheckpointableInputFormat<FileInputSplit, Tuple2<Long,Long>> {



	private static final Log LOG = LogFactory.getLog(BitcoinBlockFlinkInputFormat.class.getName());
	/**
	 *
	 */
	private static final long serialVersionUID = 4150883073922261077L;
	private boolean isEndReached;
	private transient BitcoinBlock currentBitcoinBlock;
	private long currentTransactionCounterInBlock;

	public BitcoinTransactionFlinkInputFormat(int maxSizeBitcoinBlock,String specificMagicStr,
											  boolean useDirectBuffer) throws HadoopCryptoLedgerConfigurationException {
		this(maxSizeBitcoinBlock,  specificMagicStr, useDirectBuffer,false);
	}

	public BitcoinTransactionFlinkInputFormat(int maxSizeBitcoinBlock,String specificMagicStr,
											  boolean useDirectBuffer, boolean readAuxPOW) throws HadoopCryptoLedgerConfigurationException {
		super(maxSizeBitcoinBlock,  specificMagicStr, useDirectBuffer,readAuxPOW);
		this.isEndReached=false;
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
	public Tuple2<Long,Long> getCurrentState() throws IOException {
		return new Tuple2<>(this.stream.getPos(), this.currentTransactionCounterInBlock);
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
	public void reopen(FileInputSplit split, Tuple2<Long,Long> state) throws IOException {
		try {
			this.open(split);
		} finally {
			this.stream.seek(state.f0);
			this.currentTransactionCounterInBlock=state.f1;
		}
	}

	@Override
	public BitcoinTransaction nextRecord(BitcoinTransaction reuse) throws IOException {
		BitcoinTransaction currentTransaction=null;
		if ((this.currentSplit.getLength()<0) ||(this.stream.getPos()<=this.currentSplit.getStart()+this.currentSplit.getLength())) {
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
				this.isEndReached=true;
			} else {
				currentTransaction=currentBitcoinBlock.getTransactions().get((int) currentTransactionCounterInBlock);
				currentTransactionCounterInBlock++;
			}
		} else {
			this.isEndReached=true;
		}
		return currentTransaction;
	}

}