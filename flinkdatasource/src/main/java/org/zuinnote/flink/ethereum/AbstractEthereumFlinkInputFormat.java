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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.zuinnote.hadoop.ethereum.format.common.EthereumBlockReader;
import org.zuinnote.hadoop.ethereum.format.mapreduce.AbstractEthereumRecordReader;

public abstract class AbstractEthereumFlinkInputFormat<E> extends FileInputFormat<E> {
	private static final Log LOG = LogFactory.getLog(AbstractEthereumFlinkInputFormat.class.getName());

	private transient EthereumBlockReader ebr;
	private int maxSizeEthereumBlock;
	private boolean useDirectBuffer;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5750478952540634456L;
	public AbstractEthereumFlinkInputFormat()  {
		this(AbstractEthereumRecordReader.DEFAULT_MAXSIZE_ETHEREUMBLOCK,AbstractEthereumRecordReader.DEFAULT_USEDIRECTBUFFER);
	}
	
	
	public AbstractEthereumFlinkInputFormat(int maxSizeEthereumBlock, boolean useDirectBuffer) {
		this.unsplittable=true;
		this.maxSizeEthereumBlock=maxSizeEthereumBlock;
		this.useDirectBuffer=useDirectBuffer;
	}
	
	
	/*
	 * Reads data supplied by Flink with @see org.zuinnote.hadoop.ethereum.format.common.EthereumBlockReader
	 * 
	 * (non-Javadoc)
	 * @see org.apache.flink.api.common.io.FileInputFormat#open(org.apache.flink.core.fs.FileInputSplit)
	 */
	
	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		LOG.debug("Initialize Ethereum reader");
		// temporary measure to set buffer size to 1, otherwise we cannot guarantee that checkpointing works
		ebr = new EthereumBlockReader(this.stream,this.maxSizeEthereumBlock,1,this.useDirectBuffer);
	}

	
	
	/*
	 * Returns the EthereumBlockReader used to parse this stream
	 * 
	 * @return EthereumBlockReader
	 * 
	 */
	public EthereumBlockReader getEbr() {
		return this.ebr;
}

}
