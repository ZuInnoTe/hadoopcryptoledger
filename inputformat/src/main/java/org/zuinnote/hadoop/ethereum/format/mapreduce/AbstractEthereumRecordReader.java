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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.zuinnote.hadoop.ethereum.format.common.EthereumBlockReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**

 *
 */
public abstract class AbstractEthereumRecordReader<K,V> extends RecordReader<K,V> {
	public static final String CONF_BUFFERSIZE="io.file.buffer.size";
	public static final String CONF_MAXBLOCKSIZE="hadoopcryptoledger.ethereumblockinputformat.maxblocksize";
	public static final String CONF_USEDIRECTBUFFER="hadoopcryptoledeger.ethereumblockinputformat.usedirectbuffer";
	public static final int DEFAULT_BUFFERSIZE=64*1024;
	public static final int DEFAULT_MAXSIZE_ETHEREUMBLOCK=1 * 1024 * 1024;
	public static final boolean DEFAULT_USEDIRECTBUFFER=false;
	
	private static final Log LOG = LogFactory.getLog(AbstractEthereumRecordReader.class.getName());
	private int bufferSize=0;
	private int maxSizeEthereumBlock=0;
	private boolean useDirectBuffer=false;
	private CompressionCodec codec;
	private Decompressor decompressor;
	private long start;
	private long end;
	private Seekable filePosition;
	private EthereumBlockReader ebr;
	
	/***
	 * Creates an Abstract RecordReader for Ethereum blocks
	 * 
	 * 
	 * @param conf Configuration:
	 * io.file.buffer.size: Size of in-memory  specified in the given Configuration. If io.file.buffer.size is not specified the default buffersize will be used. Furthermore, one may specify hadoopcryptoledger.ethereumblockinputformat.maxblocksize, which defines the maximum size a Ethereum block may have. By default it is 1M). If you want to experiment with performance using DirectByteBuffer instead of HeapByteBuffer you can use "hadoopcryptoledeger.ethereumblockinputformat.usedirectbuffer" (default: false). Note that it might have some unwanted consequences such as circumwenting Yarn memory management. The option is experimental and might be removed in future versions. 

	 */
	public AbstractEthereumRecordReader(Configuration conf) {
		this.maxSizeEthereumBlock=conf.getInt(AbstractEthereumRecordReader.CONF_MAXBLOCKSIZE,AbstractEthereumRecordReader.DEFAULT_MAXSIZE_ETHEREUMBLOCK);
		this.bufferSize=conf.getInt(AbstractEthereumRecordReader.CONF_BUFFERSIZE,AbstractEthereumRecordReader.DEFAULT_BUFFERSIZE);
		this.useDirectBuffer=conf.getBoolean(AbstractEthereumRecordReader.CONF_USEDIRECTBUFFER,AbstractEthereumRecordReader.DEFAULT_USEDIRECTBUFFER);

	}
	
	/***
	 * Initializes readers
	 * 
	 * @param split Split to be used (asssumed to be a file split)
	 * œüaram context context of the job
	 * @throws java.io.IOException in case of errors reading from the filestream provided by Hadoop
	 * @throws java.lang.InterruptedException in case of thread interruption
	 * 
	 */
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		   FileSplit fSplit = (FileSplit)split;
		   // Initialize start and end of split
		      start = fSplit.getStart();
		      end = start + fSplit.getLength();
		      final Path file = fSplit.getPath();
		      codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
		      final FileSystem fs = file.getFileSystem(context.getConfiguration());
		      FSDataInputStream fileIn = fs.open(file);
		      // open stream
		        if (isCompressedInput()) { // decompress
		        	decompressor = CodecPool.getDecompressor(codec);
		        	if (codec instanceof SplittableCompressionCodec) {
		  		
		          	final SplitCompressionInputStream cIn =((SplittableCompressionCodec)codec).createInputStream(fileIn, decompressor, start, end,SplittableCompressionCodec.READ_MODE.CONTINUOUS);
		  				ebr = new EthereumBlockReader(cIn, this.maxSizeEthereumBlock,this.bufferSize,this.useDirectBuffer);
		  				start = cIn.getAdjustedStart();
		         		end = cIn.getAdjustedEnd();
		          	filePosition = cIn; // take pos from compressed stream
		        } else {
		        	ebr = new EthereumBlockReader(codec.createInputStream(fileIn,decompressor), this.maxSizeEthereumBlock,this.bufferSize,this.useDirectBuffer);
		        	filePosition = fileIn;
		        }
		      } else {
		        fileIn.seek(start);
		        ebr = new EthereumBlockReader(fileIn, this.maxSizeEthereumBlock,this.bufferSize,this.useDirectBuffer);
		        filePosition = fileIn;
		      }
	}
	/**
	* Get the current Block Reader
	*
	* @return end of file position
	*
	*/
	public EthereumBlockReader getEbr() {
		return this.ebr;
	}


	/*
	* Returns how much of the file has been processed in terms of bytes
	*
	* @return progress percentage
	*
	* @throws java.io.IOException in case of errors reading from the filestream provided by Hadoop
	*
	*/
	@Override
	public  float getProgress() throws IOException {
	if (start == end) {
	      return 0.0f;
	    } else {
	      return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
	    }
	}

	/**
	* Get the end of file
	*
	* @return end of file position
	*
	*/

	public long getEnd() {
		return end;
	}

	/**
	* Returns current position in file
	*
	* @return current position in file
	*
	*
	*/

	public long getFilePosition() throws IOException {
		return this.filePosition.getPos();
	}


	/*
	* Clean up InputStream and Decompressor after use
	*
	*
	* @throws java.io.IOException in case of errors reading from the filestream provided by Hadoop
	*
	*/
	@Override
	public synchronized void  close() throws IOException {
	try {
	   if (this.ebr != null) {
	        ebr.close();
	      }
	    } finally {
	      if (decompressor != null) {
	        CodecPool.returnDecompressor(decompressor);
	        decompressor = null;
	      }
	    }
	  }


	/*
	* Determines if the input is compressed or not
	*
	* @return true if compressed, false if not
	*/
	private boolean  isCompressedInput() {
	    return codec != null;
	}

}
