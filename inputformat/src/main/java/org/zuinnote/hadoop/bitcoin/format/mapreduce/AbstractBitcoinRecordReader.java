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

package org.zuinnote.hadoop.bitcoin.format.mapreduce;



import org.zuinnote.hadoop.bitcoin.format.exception.HadoopCryptoLedgerConfigurationException;
import org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.bitcoin.format.common.*;

public abstract class AbstractBitcoinRecordReader<K,V> extends RecordReader<K,V> {
public static final String CONF_BUFFERSIZE="io.file.buffer.size";
public static final String CONF_MAXBLOCKSIZE="hadoopcryptoledger.bitcoinblockinputformat.maxblocksize";
public static final String CONF_FILTERMAGIC="hadoopcryptoledger.bitcoinblockinputformat.filter.magic";
public static final String CONF_USEDIRECTBUFFER="hadoopcryptoledeger.bitcoinblockinputformat.usedirectbuffer";
public static final String CONF_READAUXPOW="hadoopcryptoledger.bitcoinblockinputformat.readauxpow";
public static final int DEFAULT_BUFFERSIZE=64*1024;
public static final int DEFAULT_MAXSIZE_BITCOINBLOCK=8 * 1024 * 1024;
public static final String DEFAULT_MAGIC = "F9BEB4D9";
public static final boolean DEFAULT_USEDIRECTBUFFER=false;
public static final boolean DEFAULT_READAUXPOW=false;

private static final Log LOG = LogFactory.getLog(AbstractBitcoinRecordReader.class.getName());

private int bufferSize=0;
private int maxSizeBitcoinBlock=0;
private boolean useDirectBuffer=false;
private boolean readAuxPOW=false;
private String specificMagic="";
private String[] specificMagicStringArray;
private byte[][] specificMagicByteArray;

private CompressionCodec codec;
private Decompressor decompressor;
private long start;
private long end;
private Seekable filePosition;
private BitcoinBlockReader bbr;




/**
* Creates an Abstract Record Reader for Bitcoin blocks
* @param conf Configuration:
* io.file.buffer.size: Size of in-memory  specified in the given Configuration. If io.file.buffer.size is not specified the default buffersize (maximum size of a bitcoin block) will be used. The configuration hadoopcryptoledger.bitcoinblockinputformat.filter.magic allows specifying the magic identifier of the block. The magic is a comma-separated list of Hex-values (e.g. F9BEB4D9,FABFB5DA,0B110907,0B110907). The default magic is always F9BEB4D9. One needs to specify at least one magic, otherwise it will be difficult to find blocks in splits. Furthermore, one may specify hadoopcryptoledger.bitcoinblockinputformat.maxblocksize, which defines the maximum size a bitcoin block may have. By default it is 8M). If you want to experiment with performance using DirectByteBuffer instead of HeapByteBuffer you can use "hadoopcryptoledeger.bitcoinblockinputformat.usedirectbuffer" (default: false). Note that it might have some unwanted consequences such as circumwenting Yarn memory management. The option is experimental and might be removed in future versions.
*
* @throws org.zuinnote.hadoop.bitcoin.format.exception.HadoopCryptoLedgerConfigurationException in case of an invalid HadoopCryptoLedger-specific configuration of the inputformat
*
*/
public AbstractBitcoinRecordReader(Configuration conf) throws HadoopCryptoLedgerConfigurationException {
    // parse configuration
  	this.maxSizeBitcoinBlock=conf.getInt(AbstractBitcoinRecordReader.CONF_MAXBLOCKSIZE,AbstractBitcoinRecordReader.DEFAULT_MAXSIZE_BITCOINBLOCK);
	this.bufferSize=conf.getInt(AbstractBitcoinRecordReader.CONF_BUFFERSIZE,AbstractBitcoinRecordReader.DEFAULT_BUFFERSIZE);
	this.specificMagic=conf.get(AbstractBitcoinRecordReader.CONF_FILTERMAGIC);
	// we need to provide at least
	if ((this.specificMagic==null) || (this.specificMagic.length()==0)) {
 this.specificMagic=AbstractBitcoinRecordReader.DEFAULT_MAGIC;
	}
	if ((this.specificMagic!=null) && (this.specificMagic.length()>0)) {
		this.specificMagicStringArray=specificMagic.split(",");
		specificMagicByteArray=new byte[specificMagicStringArray.length][4]; // each magic is always 4 byte
		for (int i=0;i<specificMagicStringArray.length;i++) {
				byte[] currentMagicNo=BitcoinUtil.convertHexStringToByteArray(specificMagicStringArray[i]);
				if (currentMagicNo.length!=4) {
					throw new HadoopCryptoLedgerConfigurationException("Error: Configuration. Magic number has not a length of 4 bytes. Index: "+i);
				}
				specificMagicByteArray[i]=currentMagicNo;
		}
	}
	this.useDirectBuffer=conf.getBoolean(AbstractBitcoinRecordReader.CONF_USEDIRECTBUFFER,AbstractBitcoinRecordReader.DEFAULT_USEDIRECTBUFFER);
	this.readAuxPOW=conf.getBoolean(AbstractBitcoinRecordReader.CONF_READAUXPOW,AbstractBitcoinRecordReader.DEFAULT_READAUXPOW);
}


/**
* Initializes reader
* @param split Split to use (assumed to be a file split)
* @param context context of the job
*
*
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
				bbr = new BitcoinBlockReader(cIn, this.maxSizeBitcoinBlock,this.bufferSize,this.specificMagicByteArray,this.useDirectBuffer,this.readAuxPOW);
		start = cIn.getAdjustedStart();
       		end = cIn.getAdjustedEnd();
        	filePosition = cIn; // take pos from compressed stream
      } else {
	bbr = new BitcoinBlockReader(codec.createInputStream(fileIn,decompressor), this.maxSizeBitcoinBlock,this.bufferSize,this.specificMagicByteArray,this.useDirectBuffer,readAuxPOW);
	filePosition = fileIn;
      }
    } else {
      fileIn.seek(start);
      bbr = new BitcoinBlockReader(fileIn, this.maxSizeBitcoinBlock,this.bufferSize,this.specificMagicByteArray,this.useDirectBuffer,readAuxPOW);
      filePosition = fileIn;
    }
    // seek to block start (for the case a block overlaps a split)
    try {
    	bbr.seekBlockStart();
    } catch (BitcoinBlockReadException bbre) {
		LOG.error("Error reading Bitcoin blockchhain data");
		LOG.error(bbre);
    }
}







/**
* Get the current Block Reader
*
* @return end of file position
*
*/
public BitcoinBlockReader getBbr() {
	return this.bbr;
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
   if (bbr != null) {
        bbr.close();
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
