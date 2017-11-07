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
package org.zuinnote.flink.ethereum;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.io.CheckpointableInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.hadoop.io.BytesWritable;
import org.zuinnote.hadoop.ethereum.format.exception.EthereumBlockReadException;


public class EthereumRawBlockFlinkInputFormat extends AbstractEthereumFlinkInputFormat<BytesWritable> implements CheckpointableInputFormat<FileInputSplit, Long>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 8890497690418004762L;
	private static final Log LOG = LogFactory.getLog(EthereumRawBlockFlinkInputFormat.class.getName());
	private boolean isEndReached;
	
	
	/***
	 * 
	 * @param maxSizeEthereumBlock
	 * @param useDirectBuffer
	 */
	
	public EthereumRawBlockFlinkInputFormat(int maxSizeEthereumBlock, boolean useDirectBuffer) {
		super(maxSizeEthereumBlock, useDirectBuffer);
		this.isEndReached=false;
	}

	
	
	@Override
	public boolean reachedEnd() throws IOException {
		return this.isEndReached;
	}

	@Override
	public BytesWritable nextRecord(BytesWritable reuse) throws IOException {
		ByteBuffer dataBlock=null;
		if ((this.currentSplit.getLength()<0) ||(this.stream.getPos()<=this.currentSplit.getStart()+this.currentSplit.getLength())) {

				try {
					dataBlock=this.getEbr().readRawBlock();
				} catch (EthereumBlockReadException e) {
					LOG.error(e);
					throw new RuntimeException(e.toString());
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

}
