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

package org.zuinnote.hadoop.bitcoin.format.mapred;



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
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;


import org.zuinnote.hadoop.bitcoin.format.common.*;

public abstract class AbstractBitcoinRecordReader<K,V> implements RecordReader<K,V> {
public static final String CONF_BUFFERSIZE=org.zuinnote.hadoop.bitcoin.format.mapreduce.AbstractBitcoinRecordReader.CONF_BUFFERSIZE;
public static final String CONF_MAXBLOCKSIZE=org.zuinnote.hadoop.bitcoin.format.mapreduce.AbstractBitcoinRecordReader.CONF_MAXBLOCKSIZE;
public static final String CONF_FILTERMAGIC=org.zuinnote.hadoop.bitcoin.format.mapreduce.AbstractBitcoinRecordReader.CONF_FILTERMAGIC;
public static final String CONF_USEDIRECTBUFFER=org.zuinnote.hadoop.bitcoin.format.mapreduce.AbstractBitcoinRecordReader.CONF_USEDIRECTBUFFER;
public static final String CONF_READAUXPOW=org.zuinnote.hadoop.bitcoin.format.mapreduce.AbstractBitcoinRecordReader.CONF_READAUXPOW;
public static final int DEFAULT_BUFFERSIZE=org.zuinnote.hadoop.bitcoin.format.mapreduce.AbstractBitcoinRecordReader.DEFAULT_BUFFERSIZE;
public static final int DEFAULT_MAXSIZE_BITCOINBLOCK=org.zuinnote.hadoop.bitcoin.format.mapreduce.AbstractBitcoinRecordReader.DEFAULT_MAXSIZE_BITCOINBLOCK;
public static final String DEFAULT_MAGIC = org.zuinnote.hadoop.bitcoin.format.mapreduce.AbstractBitcoinRecordReader.DEFAULT_MAGIC;
public static final boolean DEFAULT_USEDIRECTBUFFER=org.zuinnote.hadoop.bitcoin.format.mapreduce.AbstractBitcoinRecordReader.DEFAULT_USEDIRECTBUFFER;
public static final boolean DEFAULT_READAUXPOW=org.zuinnote.hadoop.bitcoin.format.mapreduce.AbstractBitcoinRecordReader.DEFAULT_READAUXPOW;

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
private Reporter reporter;
private Configuration conf;
private long start;
private long end;
private final Seekable filePosition;
private FSDataInputStream fileIn;
private BitcoinBlockReader bbr;




/**
* Creates an Abstract Record Reader for Bitcoin blocks
* @param split Split to use (assumed to be a file split)
* @param job Configuration:
* io.file.buffer.size: Size of in-memory  specified in the given Configuration. If io.file.buffer.size is not specified the default buffersize (maximum size of a bitcoin block) will be used. The configuration hadoopcryptoledger.bitcoinblockinputformat.filter.magic allows specifying the magic identifier of the block. The magic is a comma-separated list of Hex-values (e.g. F9BEB4D9,FABFB5DA,0B110907,0B110907). The default magic is always F9BEB4D9. One needs to specify at least one magic, otherwise it will be difficult to find blocks in splits. Furthermore, one may specify hadoopcryptoledger.bitcoinblockinputformat.maxblocksize, which defines the maximum size a bitcoin block may have. By default it is 8M). If you want to experiment with performance using DirectByteBuffer instead of HeapByteBuffer you can use "hadoopcryptoledeger.bitcoinblockinputformat.usedirectbuffer" (default: false). Note that it might have some unwanted consequences such as circumwenting Yarn memory management. The option is experimental and might be removed in future versions. 
* @param reporter Reporter
*
*
* @throws java.io.IOException in case of errors reading from the filestream provided by Hadoop
* @throws org.zuinnote.hadoop.bitcoin.format.exception.HadoopCryptoLedgerConfigurationException in case of an invalid HadoopCryptoLedger-specific configuration of the inputformat
* @throws org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException in case the Bitcoin data contains invalid blocks (e.g. magic might be different)
*
*/
public AbstractBitcoinRecordReader(FileSplit split,JobConf job, Reporter reporter) throws IOException,HadoopCryptoLedgerConfigurationException,BitcoinBlockReadException {
    LOG.debug("Reading configuration");
    // parse configuration
     this.reporter=reporter;
     this.conf=job;	
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
    // Initialize start and end of split
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    codec = new CompressionCodecFactory(job).getCodec(file);
    final FileSystem fs = file.getFileSystem(job);
    fileIn = fs.open(file);
    // open stream
      if (isCompressedInput()) { // decompress
	LOG.debug("Decompressing file");
      	decompressor = CodecPool.getDecompressor(codec);
      	if (codec instanceof SplittableCompressionCodec) {
		LOG.debug("SplittableCompressionCodec");
        	final SplitCompressionInputStream cIn =((SplittableCompressionCodec)codec).createInputStream(fileIn, decompressor, start, end,SplittableCompressionCodec.READ_MODE.CONTINUOUS);
		bbr = new BitcoinBlockReader(cIn, this.maxSizeBitcoinBlock,this.bufferSize,this.specificMagicByteArray,this.useDirectBuffer,this.readAuxPOW);  
		start = cIn.getAdjustedStart();
       		end = cIn.getAdjustedEnd();
        	filePosition = cIn; // take pos from compressed stream
      } else {
	LOG.debug("Not-splitable compression codec");
	bbr = new BitcoinBlockReader(codec.createInputStream(fileIn,decompressor), this.maxSizeBitcoinBlock,this.bufferSize,this.specificMagicByteArray,this.useDirectBuffer,this.readAuxPOW);
        filePosition = fileIn;
      }
    } else {
      LOG.debug("Processing file without compression");
      fileIn.seek(start);
      bbr = new BitcoinBlockReader(fileIn, this.maxSizeBitcoinBlock,this.bufferSize,this.specificMagicByteArray,this.useDirectBuffer,this.readAuxPOW);  
      filePosition = fileIn;
    }
    // initialize reader
    // seek to block start (for the case a block overlaps a split)
    LOG.debug("Seeking to block start");
    this.reporter.setStatus("Seeking Block start");
    bbr.seekBlockStart();
    this.reporter.setStatus("Ready to read");
}




/**
* Get the current file position in a compressed or uncompressed file.
*
* @return file position
*
* @throws java.io.IOException in case of errors reading from the filestream provided by Hadoop
*
*/

public long getFilePosition() throws IOException {
	return  filePosition.getPos();
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
public synchronized float getProgress() throws IOException {
if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
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

/*
* Get current position in the stream
*
* @return position
*
* @throws java.io.IOException in case of errors reading from the filestream provided by Hadoop
*
*/
@Override
public  synchronized long getPos() throws IOException {
	return filePosition.getPos();
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
}
