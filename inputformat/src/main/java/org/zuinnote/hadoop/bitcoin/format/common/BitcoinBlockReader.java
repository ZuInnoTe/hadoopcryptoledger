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

package org.zuinnote.hadoop.bitcoin.format.common;

import org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException;

import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedInputStream;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import java.util.List;
import java.util.ArrayList;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
* This class reads Bitcoin blocks (in raw network format) from an input stream and returns Java objects of the class BitcoinBlock. It reuses code from the LineRecordReader due to its robustness and well-tested functionality.
*
**/

public class BitcoinBlockReader {

private static final Log LOG = LogFactory.getLog(BitcoinBlockReader.class.getName());

private int maxSizeBitcoinBlock=0; 
private boolean useDirectBuffer=false;

private boolean filterSpecificMagic=false;
private byte[][] specificMagicByteArray;
private ByteBuffer preAllocatedDirectByteBuffer;

private BufferedInputStream bin;
/**
* Create a BitcoinBlock reader that reads from the given stream and uses the given parameters for configuration. Note it assumed that the validity of this configuration is checked by BitcoinBlockRecordReader
* @param in Input stream to read from
* @param maxSizeBitcoinBlock Maximum size of a Bitcoinblock.
* @param bufferSize size of the memory buffer for the givenInputStream
* @param specificMagicByteArray filters by specific block magic numbers if not null. 
* @param useDirectBuffer experimental feature to use a DirectByteBuffer instead of a HeapByteBuffer
**/

public BitcoinBlockReader(InputStream in, int maxSizeBitcoinBlock, int bufferSize, byte[][] specificMagicByteArray, boolean useDirectBuffer) {
	this.maxSizeBitcoinBlock=maxSizeBitcoinBlock;
	this.specificMagicByteArray=specificMagicByteArray;
	this.useDirectBuffer=useDirectBuffer;
	if (specificMagicByteArray!=null) {
		this.filterSpecificMagic=true;
	}
	this.bin=new BufferedInputStream(in,bufferSize);
	if (this.useDirectBuffer) { // in case of a DirectByteBuffer we do allocation only once for the maximum size of one block, otherwise we will have a high cost for reallocation
		preAllocatedDirectByteBuffer=ByteBuffer.allocateDirect(this.maxSizeBitcoinBlock);
	}
}


/**
* Seek for a valid block start according to the following algorithm:
* (1) find the magic of the block 
* (2) Check that the block can be fully read and that block size is smaller than maximum block size
* This functionality is particularly useful for file processing in Big Data systems, such as Hadoop and Co where we work indepently on different filesplits and cannot expect that the Bitcoin block starts directly at the beginning of the stream;
* 
* @throws org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException in case of format errors of the Bitcoin Blockchain data
*
**/

public void seekBlockStart() throws BitcoinBlockReadException {
	if (!(this.filterSpecificMagic)) {
		throw new BitcoinBlockReadException("Error: Cannot seek to a block start, because no magic(s) are defined.");
	}
	findMagic();
	// validate it is a full block
	checkFullBlock();
}

/**
* Read a block into a Java object of the class Bitcoin Block. This makes analysis very easy, but might be slower for some type of analytics where you are only interested in small parts of the block. In this case it is recommended to use {@link #readRawBlock}
*
* @return BitcoinBlock
* @throws org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException in case of errors of reading the Bitcoin Blockchain data
*/

public BitcoinBlock readBlock() throws BitcoinBlockReadException {
  	ByteBuffer rawByteBuffer = readRawBlock();
	if (rawByteBuffer==null) {
		return null;
	}
	// start parsing
	// initialize byte arrays
	byte[] currentMagicNo=new byte[4];	
	byte[] currentBits=new byte[4];
	byte[] currentHashMerkleRoot=new byte[32];
	byte[] currentHashPrevBlock=new byte[32];
	// magic no
	rawByteBuffer.get(currentMagicNo,0,4);
	// blocksize
	int currentBlockSize=rawByteBuffer.getInt();
	// version
	int currentVersion=rawByteBuffer.getInt();
	// hashPrevBlock
	rawByteBuffer.get(currentHashPrevBlock,0,32);
	// hashMerkleRoot
	rawByteBuffer.get(currentHashMerkleRoot,0,32);	
	// time 
	int currentTime=rawByteBuffer.getInt();
	// bits/difficulty
	rawByteBuffer.get(currentBits,0,4);
	// nonce
	int currentNonce=rawByteBuffer.getInt();
	// read var int from transaction counter
		
	long currentTransactionCounter=BitcoinUtil.convertVarIntByteBufferToLong(rawByteBuffer);
	// parse transactions 
	List<BitcoinTransaction> allBlockTransactions=parseTransactions(rawByteBuffer,currentTransactionCounter);
	if (allBlockTransactions.size()!=currentTransactionCounter) {
		 throw new BitcoinBlockReadException("Error: Number of Transactions ("+allBlockTransactions.size()+") does not correspond to transaction counter in block ("+currentTransactionCounter+")");
	}
	BitcoinBlock result=new BitcoinBlock();
	result.setMagicNo(currentMagicNo);
	result.setBlockSize(currentBlockSize);
	result.setVersion(currentVersion);
	result.setTime(currentTime);
	result.setBits(currentBits);
	result.setNonce(currentNonce);
	result.setTransactionCounter(currentTransactionCounter);
	result.setHashPrevBlock(currentHashPrevBlock);
	result.setHashMerkleRoot(currentHashMerkleRoot);
	result.setTransactions(allBlockTransactions);
	return result;
}

/**
* Parses the Bitcoin transactions in a byte buffer. 
*
* @param rawByteBuffer ByteBuffer from which the transactions have to be parsed
* @param noOfTransactions Number of expected transactions
*
* @return Array of transactions
*
*
*/

public List<BitcoinTransaction> parseTransactions(ByteBuffer rawByteBuffer,long noOfTransactions) {
	ArrayList<BitcoinTransaction> resultTransactions = new ArrayList<>((int)noOfTransactions);
	// read all transactions from ByteBuffer
	for (int k=0;k<noOfTransactions;k++) {
		// read version
		int currentVersion=rawByteBuffer.getInt();
		// read inCounter
		byte[] currentInCounterVarInt=BitcoinUtil.convertVarIntByteBufferToByteArray(rawByteBuffer);
		long currentNoOfInputs=BitcoinUtil.getVarInt(currentInCounterVarInt);
		// read inputs
		ArrayList<BitcoinTransactionInput> currentTransactionInput = new ArrayList<>((int)currentNoOfInputs);
		
		for (int i=0;i<currentNoOfInputs;i++) {
			// read previous Hash of Transaction
			byte[] currentTransactionInputPrevTransactionHash=new byte[32];
			rawByteBuffer.get(currentTransactionInputPrevTransactionHash,0,32);
			// read previousTxOutIndex
			long currentTransactionInputPrevTxOutIdx=BitcoinUtil.convertSignedIntToUnsigned(rawByteBuffer.getInt());
			// read InScript length (Potential Internal Exceed Java Type)
			byte[] currentTransactionTxInScriptLengthVarInt=BitcoinUtil.convertVarIntByteBufferToByteArray(rawByteBuffer);
			long currentTransactionTxInScriptSize=BitcoinUtil.getVarInt(currentTransactionTxInScriptLengthVarInt);
			// read inScript
			int currentTransactionTxInScriptSizeInt=(int)currentTransactionTxInScriptSize;
			byte[] currentTransactionInScript=new byte[currentTransactionTxInScriptSizeInt];
			rawByteBuffer.get(currentTransactionInScript,0,currentTransactionTxInScriptSizeInt);
			// read sequence no
			long currentTransactionInputSeqNo=BitcoinUtil.convertSignedIntToUnsigned(rawByteBuffer.getInt());
			// add input
			currentTransactionInput.add(new BitcoinTransactionInput(currentTransactionInputPrevTransactionHash,currentTransactionInputPrevTxOutIdx,currentTransactionTxInScriptLengthVarInt,currentTransactionInScript,currentTransactionInputSeqNo));	
		}
		// read outCounter
		byte[] currentOutCounterVarInt=BitcoinUtil.convertVarIntByteBufferToByteArray(rawByteBuffer);
		long currentNoOfOutput=BitcoinUtil.getVarInt(currentOutCounterVarInt);
		// read outputs
		ArrayList<BitcoinTransactionOutput> currentTransactionOutput = new ArrayList<>((int)(currentNoOfOutput));
		for (int i=0;i<currentNoOfOutput;i++) {
			// read value
			long currentTransactionOutputValue = rawByteBuffer.getLong();
			// read outScript length (Potential Internal Exceed Java Type)
			byte[] currentTransactionTxOutScriptLengthVarInt=BitcoinUtil.convertVarIntByteBufferToByteArray(rawByteBuffer);
			long currentTransactionTxOutScriptSize=BitcoinUtil.getVarInt(currentTransactionTxOutScriptLengthVarInt);
			int currentTransactionTxOutScriptSizeInt=(int)(currentTransactionTxOutScriptSize);
			// read outScript
			byte[] currentTransactionOutScript=new byte[currentTransactionTxOutScriptSizeInt];
			rawByteBuffer.get(currentTransactionOutScript,0,currentTransactionTxOutScriptSizeInt);
			currentTransactionOutput.add(new BitcoinTransactionOutput(currentTransactionOutputValue,currentTransactionTxOutScriptLengthVarInt,currentTransactionOutScript));
		}
		// lock_time
		int currentTransactionLockTime = rawByteBuffer.getInt();
		// add transaction
		resultTransactions.add(new BitcoinTransaction(currentVersion,currentInCounterVarInt,currentTransactionInput,currentOutCounterVarInt,currentTransactionOutput,currentTransactionLockTime));
	}
	return resultTransactions;
}

/*
* Reads a raw Bitcoin block into a ByteBuffer. This method is recommended if you are only interested in a small part of the block and do not need the deserialization of the full block, ie in case you generally skip a lot of blocks
*
*
* @return ByteBuffer containing the block
*
* @throws org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException in case of format errors of the Bitcoin Blockchain data
**/


public ByteBuffer readRawBlock() throws BitcoinBlockReadException {
  try {
	byte[] blockSizeByte = new byte[0];
	while (blockSizeByte.length==0) { // in case of filtering by magic no we skip blocks until we reach a valid magicNo or end of Block
		// check if more to read
		if (this.bin.available()<1) {
			return null;
		}
		blockSizeByte=skipBlocksNotInFilter();
	}
	// check if it is larger than maxsize, include 8 bytes for the magic and size header
	long blockSize=BitcoinUtil.getSize(blockSizeByte)+8;
	if (blockSize==0) {
		throw new BitcoinBlockReadException("Error: Blocksize too small");
	}
	if (blockSize<0) {
		throw new BitcoinBlockReadException("Error: This block size cannot be handled currently (larger then largest number in positive signed int)");
	}
	if (blockSize>this.maxSizeBitcoinBlock) {
		throw new BitcoinBlockReadException("Error: Block size is larger then defined in configuration - Please increase it if this is a valid block");
	}
	// read full block into ByteBuffer
	int blockSizeInt=(int)(blockSize);
	byte[] fullBlock=new byte[blockSizeInt];
	int totalByteRead=0;
	int readByte;
	while ((readByte=this.bin.read(fullBlock,totalByteRead,blockSizeInt-totalByteRead))>-1) {
			totalByteRead+=readByte;
			if (totalByteRead>=blockSize) {
				break;
			}
	}
	if (totalByteRead!=blockSize) {
		 throw new BitcoinBlockReadException("Error: Could not read full block");
	}
	ByteBuffer result;
	if (!(this.useDirectBuffer)) {
	 	result=ByteBuffer.wrap(fullBlock);	
	} else {
		preAllocatedDirectByteBuffer.clear(); // clear out old bytebuffer
		preAllocatedDirectByteBuffer.limit(fullBlock.length); // limit the bytebuffer
		result=preAllocatedDirectByteBuffer;
		result.put(fullBlock);
		result.flip(); // put in read mode
	}
	result.order(ByteOrder.LITTLE_ENDIAN);	
	return result;
  } catch (IOException e) {
	LOG.error(e);
	throw new BitcoinBlockReadException(e.toString());
  }
}

/**
* This function is used to read from a raw Bitcoin block some identifier. Note: Does not change ByteBuffer position
*
* @param rawByteBuffer ByteBuffer as read by readRawBlock
* @return byte array containing hashMerkleRoot and prevHashBlock
*
*/
public byte[] getKeyFromRawBlock (ByteBuffer rawByteBuffer)  {
	rawByteBuffer.mark();
	byte[] magicNo=new byte[4];
	byte[] hashMerkleRoot=new byte[32];
	byte[] hashPrevBlock=new byte[32];
	// magic no (skip)
	rawByteBuffer.get(magicNo,0,4);
	// blocksize (skip)
	rawByteBuffer.getInt();
	// version (skip)
	rawByteBuffer.getInt();
	// hashPrevBlock
	rawByteBuffer.get(hashPrevBlock,0,32);
	// hashMerkleRoot
	rawByteBuffer.get(hashMerkleRoot,0,32);
	byte[] result=new byte[hashMerkleRoot.length+hashPrevBlock.length];
	for (int i=0;i<hashMerkleRoot.length;i++) {
		result[i]=hashMerkleRoot[i];
	}
	for (int j=0;j<hashPrevBlock.length;j++) {
		result[j+hashMerkleRoot.length]=hashPrevBlock[j];
	}
	rawByteBuffer.reset();
	return result;
}

/**
* Closes the reader
*
* @throws java.io.IOException in case of errors reading from the InputStream
*
*/

public void close() throws IOException {
	this.bin.close();
}


/*
* Finds the start of a block by looking for the specified magics in the current InputStream
*
* @throws org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException in case of errors reading Blockchain data
*
*/

private void findMagic() throws BitcoinBlockReadException {
boolean magicFound=false;
	// search if first byte of any magic matches
	// search up to maximum size of a bitcoin block
	int currentSeek=0;
	while(!(magicFound)) {
		int firstByte=-1;
		try {
			this.bin.mark(4); // magic is always 4 bytes
			firstByte=this.bin.read();
		} catch (IOException e) {
			LOG.error(e);
			throw new BitcoinBlockReadException(e.toString());
		}
		if (firstByte==-1) { 
			throw new BitcoinBlockReadException("Error: Did not find defined magic within current stream");
		}
		try {
			if (checkForMagicBytes(firstByte)) {
				return;
			}
		} catch (IOException e) {
			LOG.error(e);
			throw new BitcoinBlockReadException(e.toString());
		}
		if (currentSeek==this.maxSizeBitcoinBlock) { 
			throw new BitcoinBlockReadException("Error: Cannot seek to a block start, because no valid block found within the maximum size of a Bitcoin block. Check data or increase maximum size of Bitcoin block.");
		}
	// increase by one byte if magic not found yet
		try {
			this.bin.reset();
			if (this.bin.skip(1)!=1) {
				LOG.error("Error cannot skip 1 byte in InputStream");
			}
		} catch (IOException e) {
			LOG.error(e);
			throw new BitcoinBlockReadException(e.toString());
		}
	currentSeek++;
	}
}

/*
* Checks if there is a full Bitcoin Block at the current position of the InputStream
*
* @throws org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException in case of errors reading Blockchain data
*
*/

private void checkFullBlock() throws BitcoinBlockReadException {
	// now we can check that we have a full block
		try {
			this.bin.mark(this.maxSizeBitcoinBlock);
			// skip maigc
			long skipMagic=this.bin.skip(4);
			if (skipMagic!=4) {
				 throw new BitcoinBlockReadException("Error: Cannot seek to a block start, because no valid block found. Cannot skip forward magic");
			}
		}
		catch (IOException e) {
			LOG.error(e);
			throw new BitcoinBlockReadException(e.toString());
		}
		// read size
		// blocksize
		byte[] blockSizeArray = new byte[4];
		try {
			int readSize=this.bin.read(blockSizeArray,0,4);
			if (readSize!=4) {
				throw new BitcoinBlockReadException("Error: Cannot seek to a block start, because no valid block found. Cannot read size of block");
			}
		}
		catch (IOException e) {
			LOG.error(e);
			throw new BitcoinBlockReadException(e.toString());
		}		
		long blockSize=BitcoinUtil.getSize(blockSizeArray);
		if (this.maxSizeBitcoinBlock<blockSize) {
			throw new BitcoinBlockReadException("Error: Cannot seek to a block start, because no valid block found. Max bitcoin block size is smaller than current block size.");
		}
		int blockSizeInt=(int)blockSize;
		byte[] blockRead=new byte[blockSizeInt];
		int totalByteRead=0;
		int readByte;
		try {
		while ((readByte=this.bin.read(blockRead,totalByteRead,blockSizeInt-totalByteRead))>-1) {
			totalByteRead+=readByte;
			if (totalByteRead>=blockSize) { 
				break;
			}
		}
		} catch (IOException e) {
			LOG.error(e);
			throw new BitcoinBlockReadException(e.toString());
		}
		if (totalByteRead!=blockSize) {
			 throw new BitcoinBlockReadException("Error: Cannot seek to a block start, because no valid block found. Cannot skip to end of block");
		}
		try {
			this.bin.reset();
		} catch (IOException e) {
			LOG.error(e);
			throw new BitcoinBlockReadException(e.toString());
		}
		// it is a full block
}


/*
* Skips blocks in inputStream which are not specified in the magic filter
*
* @return null or byte array containing the size of the block (not the block itself)
*
* @throws java.io.IOException in case of errors reading from InputStream
*
*/
private byte[] skipBlocksNotInFilter() throws IOException {
		byte[] magicNo=new byte[4];
		byte[] blockSizeByte=new byte[4];
		// mark bytestream so we can peak into it
		this.bin.mark(8);
		// read magic
		
		int magicNoReadSize=this.bin.read(magicNo,0,4);
		if (magicNoReadSize!=4) {
			return new byte[0]; // no more magics to read
		}
		// read blocksize
	
		int blockSizeReadSize=this.bin.read(blockSizeByte,0,4);
		if (blockSizeReadSize!=4) {
			return new byte[0]; // no more size to read
		}
		long blockSize=BitcoinUtil.getSize(blockSizeByte)+8;
		// read the full block
		this.bin.reset();
		//filter by magic numbers?
		if (filterSpecificMagic) {
			for (int i=0;i<specificMagicByteArray.length;i++) {
				byte[] currentFilter=specificMagicByteArray[i];
				boolean doesMatchOneMagic=BitcoinUtil.compareMagics(currentFilter,magicNo);
				// correspond to filter? read it!
				if (doesMatchOneMagic) {
					return blockSizeByte;
				}
			}
			// Skip block if not found
			if (this.bin.skip(blockSize)!=blockSize) {
					LOG.error("Cannot skip block in InputStream");
			}
			return new byte[0];
		
		} else {
			return blockSizeByte;
		}
}

/**
* Checks in BufferedInputStream (bin) for the magic(s) specified in specificMagicByteArray
*
* @param firstByte first byte (as int) of the byteBuffer
*
* @retrun true if one of the magics has been identified, false if not
*
* @throws java.io.IOException in case of issues reading from BufferedInputStream
*
*/

private boolean checkForMagicBytes(int firstByte) throws IOException {
	byte[] fullMagic=null;
		for (int i=0;i<this.specificMagicByteArray.length;i++) {
			// compare first byte and decide if we want to read full magic
			int currentMagicFirstbyte=this.specificMagicByteArray[i][0] & 0xFF;
			if (firstByte==currentMagicFirstbyte) {
				if (fullMagic==null) { // read full magic
					fullMagic=new byte[4];
					fullMagic[0]=this.specificMagicByteArray[i][0];
					this.bin.read(fullMagic,1,3);
				}
				// compare full magics
				if (BitcoinUtil.compareMagics(fullMagic,this.specificMagicByteArray[i])) {
					this.bin.reset();
					return true;
				}
			}
				
			} 
	return false;
}


}
