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
 * Flink Data Source base for Bitcoin Input formats
 */

package org.zuinnote.flink.bitcoin;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlockReader;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinUtil;
import org.zuinnote.hadoop.bitcoin.format.exception.HadoopCryptoLedgerConfigurationException;
import org.zuinnote.hadoop.bitcoin.format.mapred.AbstractBitcoinRecordReader;

public abstract class AbstractBitcoinFlinkInputFormat<E> extends FileInputFormat<E> {

	private static final Log LOG = LogFactory.getLog(AbstractBitcoinFlinkInputFormat.class.getName());

	private transient BitcoinBlockReader bbr;
	private int maxSizeBitcoinBlock;
	private byte [][] specificMagicArray;
	private boolean useDirectBuffer;
	/**
	 * 
	 */
	private static final long serialVersionUID = -4661705676237973665L;
	
	public AbstractBitcoinFlinkInputFormat() throws HadoopCryptoLedgerConfigurationException {
		this(AbstractBitcoinRecordReader.DEFAULT_MAXSIZE_BITCOINBLOCK,AbstractBitcoinRecordReader.DEFAULT_MAGIC,AbstractBitcoinRecordReader.DEFAULT_USEDIRECTBUFFER);
	}
	
	public AbstractBitcoinFlinkInputFormat(int maxSizeBitcoinBlock, String specificMagicStr, boolean useDirectBuffer) throws HadoopCryptoLedgerConfigurationException {
		this.maxSizeBitcoinBlock=maxSizeBitcoinBlock;
		this.useDirectBuffer=useDirectBuffer;
		if ((specificMagicStr!=null) && (specificMagicStr.length()>0)) {
			String[] specificMagicStringArray=specificMagicStr.split(",");
			this.specificMagicArray=new byte[specificMagicStringArray.length][4]; // each magic is always 4 byte
			for (int i=0;i<specificMagicStringArray.length;i++) {
					byte[] currentMagicNo=BitcoinUtil.convertHexStringToByteArray(specificMagicStringArray[i]);
					if (currentMagicNo.length!=4) {
						throw new HadoopCryptoLedgerConfigurationException("Error: Configuration. Magic number has not a length of 4 bytes. Index: "+i);
					}
					this.specificMagicArray[i]=currentMagicNo;
			}
		}	
	}
	
	/*
	 * Reads data supplied by Flink with @see org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlockReader
	 * 
	 * (non-Javadoc)
	 * @see org.apache.flink.api.common.io.FileInputFormat#open(org.apache.flink.core.fs.FileInputSplit)
	 */
	
	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		LOG.debug("Initialize Bitcoin reader");
		// temporary measure to set buffer size to 1, otherwise we cannot guarantee that checkpointing works
		bbr = new BitcoinBlockReader(this.stream,this.maxSizeBitcoinBlock,1,this.specificMagicArray,this.useDirectBuffer);
	}

	
	
	/*
	 * Returns the BitcoinBlockReader used to parse this stream
	 * 
	 * @return BitcoinBlockReader
	 * 
	 */
	public BitcoinBlockReader getBbr() {
		return this.bbr;
	}
	
}