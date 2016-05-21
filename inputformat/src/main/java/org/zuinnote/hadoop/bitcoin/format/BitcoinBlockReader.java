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

package org.zuinnote.hadoop.bitcoin.format;

import org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException;

import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedInputStream;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import java.util.ArrayList;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
* This class reads Bitcoin blocks (in raw network format) from an input stream and returns Java objects of the class BitcoinBlock. It reuses code from the LineRecordReader due to its robustness and well-tested functionality.
*
**/

public class BitcoinBlockReader {

private static final Log LOG = LogFactory.getLog(BitcoinBlockReader.class.getName());

private int bufferSize=0;
private int maxSizeBitcoinBlock=0; 
private boolean useDirectBuffer=false;

private boolean filterSpecificMagic=false;
private byte[][] specificMagicByteArray;
private ByteBuffer preAllocatedDirectByteBuffer;

private BufferedInputStream bin;
private int position=0;

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
	this.bufferSize=bufferSize;
	this.specificMagicByteArray=specificMagicByteArray;
	this.useDirectBuffer=useDirectBuffer;
	if (specificMagicByteArray!=null) this.filterSpecificMagic=true;
	this.bin=new BufferedInputStream(in,bufferSize);
	if (this.useDirectBuffer==true) { // in case of a DirectByteBuffer we do allocation only once for the maximum size of one block, otherwise we will have a high cost for reallocation
		preAllocatedDirectByteBuffer=ByteBuffer.allocateDirect(this.maxSizeBitcoinBlock);
	}
}


/**
* Seek for a valid block start according to the following algorithm:
* (1) find the magic of the block 
* (2) Check that the block can be fully read and that block size is smaller than maximum block size
* This functionality is particularly useful for file processing in Big Data systems, such as Hadoop & Co where we work indepently on different filesplits and cannot expect that the Bitcoin block starts directly at the beginning of the stream;
**/

public void seekBlockStart() throws BitcoinBlockReadException,IOException {
	if (this.filterSpecificMagic==false) throw new BitcoinBlockReadException("Error: Cannot seek to a block start, because no magic(s) are defined.");
	boolean magicFound=false;
	// search if first byte of any magic matches
	// search up to maximum size of a bitcoin block
	int currentSeek=0;
	while(magicFound==false) {
		this.bin.mark(4); // magic is always 4 bytes
		int firstByte=this.bin.read();
		if (firstByte==-1) throw new BitcoinBlockReadException("Error: Did not find defined magic within current stream");
		byte[] fullMagic=null;
		for (int i=0;i<specificMagicByteArray.length;i++) {
			// compare first byte and decide if we want to read full magic
			int currentMagicFirstbyte=specificMagicByteArray[i][0] & 0xFF;
			if (firstByte==currentMagicFirstbyte) {
				if (fullMagic==null) { // read full magic
					fullMagic=new byte[4];
					fullMagic[0]=specificMagicByteArray[i][0];
					this.bin.read(fullMagic,1,3);
				}
				// compare full magics
				if (BitcoinUtil.compareMagics(fullMagic,specificMagicByteArray[i])==true) {
					magicFound=true;
					this.bin.reset();
					break;
				}
				
			} 
		}
		if (currentSeek==this.maxSizeBitcoinBlock) throw new BitcoinBlockReadException("Error: Cannot seek to a block start, because no valid block found within the maximum size of a Bitcoin block. Check data or increase maximum size of Bitcoin block.");
	// increase by one byte
	if (magicFound==false) {
		this.bin.reset();
		this.bin.skip(1);
	}
	currentSeek++;
	}
	// validate it is a full block
	boolean fullBlock=false;
	if (magicFound==true) {
		// now we can check that we have a full block
		this.bin.mark(this.maxSizeBitcoinBlock);
		// skip maigc
		long skipMagic=this.bin.skip(4);
		if (skipMagic!=4) throw new BitcoinBlockReadException("Error: Cannot seek to a block start, because no valid block found. Cannot skip forward magic");
		// read size
		// blocksize
		byte[] blockSizeArray = new byte[4];
		int readSize=this.bin.read(blockSizeArray,0,4);
		if (readSize!=4) throw new BitcoinBlockReadException("Error: Cannot seek to a block start, because no valid block found. Cannot read size of block");
		
		long blockSize=BitcoinUtil.getSize(blockSizeArray);
		if (this.maxSizeBitcoinBlock<blockSize) throw new BitcoinBlockReadException("Error: Cannot seek to a block start, because no valid block found. Max bitcoin block size is smaller than current block size.");
		int blockSizeInt=new Long(blockSize).intValue();
		byte[] blockRead=new byte[blockSizeInt];
		long readByte=this.bin.read(blockRead,0,blockSizeInt);
		if (readByte!=blockSize) throw new BitcoinBlockReadException("Error: Cannot seek to a block start, because no valid block found. Cannot skip to end of block");
		
		this.bin.reset();
		fullBlock=true;
	}
	if ((magicFound==false) || (fullBlock==false)) throw new BitcoinBlockReadException("Error: Cannot seek to a block start, because no valid block found");
}

/**
* Read a block into a Java object of the class Bitcoin Block. This makes analysis very easy, but might be slower for some type of analytics where you are only interested in small parts of the block. In this case it is recommended to use {@see #readRawBlock}
*
* @return BitcoinBlock
*/

public BitcoinBlock readBlock() throws BitcoinBlockReadException,IOException {
	ByteBuffer rawByteBuffer = readRawBlock();
	if (rawByteBuffer==null) return null;
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
	BitcoinTransaction[] allBlockTransactions=parseTransactions(rawByteBuffer,currentTransactionCounter);
	if (allBlockTransactions.length!=currentTransactionCounter) throw new BitcoinBlockReadException("Error: Number of Transactions ("+allBlockTransactions.length+") does not correspond to transaction counter in block ("+currentTransactionCounter+")");
	BitcoinBlock result=new BitcoinBlock(currentMagicNo,currentBlockSize,currentVersion,currentTime,currentBits,currentNonce,currentTransactionCounter,currentHashPrevBlock,currentHashMerkleRoot,allBlockTransactions);
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

public BitcoinTransaction[] parseTransactions(ByteBuffer rawByteBuffer,long noOfTransactions) {
	ArrayList<BitcoinTransaction> resultTransactions = new ArrayList<BitcoinTransaction>();
	// read all transactions from ByteBuffer
	for (int k=0;k<noOfTransactions;k++) {
		// read version
		int currentVersion=rawByteBuffer.getInt();
		// read inCounter
		byte[] currentInCounterVarInt=BitcoinUtil.convertVarIntByteBufferToByteArray(rawByteBuffer);
		long currentNoOfInputs=BitcoinUtil.getVarInt(currentInCounterVarInt);
		// read inputs
		ArrayList<BitcoinTransactionInput> currentTransactionInput = new ArrayList<BitcoinTransactionInput>();
		
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
			int currentTransactionTxInScriptSizeInt=new Long(currentTransactionTxInScriptSize).intValue();
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
		ArrayList<BitcoinTransactionOutput> currentTransactionOutput = new ArrayList<BitcoinTransactionOutput>();
		for (int i=0;i<currentNoOfOutput;i++) {
			// read value
			long currentTransactionOutputValue = rawByteBuffer.getLong();
			// read outScript length (Potential Internal Exceed Java Type)
			byte[] currentTransactionTxOutScriptLengthVarInt=BitcoinUtil.convertVarIntByteBufferToByteArray(rawByteBuffer);
			long currentTransactionTxOutScriptSize=BitcoinUtil.getVarInt(currentTransactionTxOutScriptLengthVarInt);
			int currentTransactionTxOutScriptSizeInt=new Long(currentTransactionTxOutScriptSize).intValue();
			// read outScript
			byte[] currentTransactionOutScript=new byte[currentTransactionTxOutScriptSizeInt];
			rawByteBuffer.get(currentTransactionOutScript,0,currentTransactionTxOutScriptSizeInt);
			currentTransactionOutput.add(new BitcoinTransactionOutput(currentTransactionOutputValue,currentTransactionTxOutScriptLengthVarInt,currentTransactionOutScript));
		}
		// lock_time
		int currentTransactionLockTime = rawByteBuffer.getInt();
		// create Transaction
			// Inputs as array
			BitcoinTransactionInput[] listOfInputs = new BitcoinTransactionInput[currentTransactionInput.size()];
			listOfInputs=currentTransactionInput.toArray(listOfInputs);
			// Outputs as array
			BitcoinTransactionOutput[] listOfOutputs = new BitcoinTransactionOutput[currentTransactionOutput.size()];
			listOfOutputs=currentTransactionOutput.toArray(listOfOutputs);
		// add transaction
		resultTransactions.add(new BitcoinTransaction(currentVersion,currentInCounterVarInt,listOfInputs,currentOutCounterVarInt,listOfOutputs,currentTransactionLockTime));
	}
	BitcoinTransaction[] result = new BitcoinTransaction[resultTransactions.size()];
	result=resultTransactions.toArray(result);
	return result;
}

/*
* Reads a raw Bitcoin block into a ByteBuffer. This method is recommended if you are only interested in a small part of the block and do not need the deserialization of the full block, ie in case you generally skip a lot of blocks
*
*
* @return ByteBuffer containing the block
**/


public ByteBuffer readRawBlock() throws BitcoinBlockReadException, IOException {
	boolean readBlock=false;
	byte[] magicNo=new byte[4];
	byte[] blockSizeByte=new byte[4];
	long blockSize=0;
	while (readBlock==false) { // in case of filtering by magic no we skip blocks until we reach a valid magicNo or end of Block
		// check if more to read
		if (this.bin.available()<1) {
			return null;
		}
		// mark bytestream so we can peak into it
		this.bin.mark(8);
		// read magic
		
		int magicNoReadSize=this.bin.read(magicNo,0,4);
		if (magicNoReadSize!=4) return null; // no more magics to read
		// read blocksize
	
		int blockSizeReadSize=this.bin.read(blockSizeByte,0,4);
		if (blockSizeReadSize!=4) return null; // no more size to read
		blockSize=BitcoinUtil.getSize(blockSizeByte)+8;
		// read the full block
		this.bin.reset();
		//filter by magic numbers?
		if (filterSpecificMagic==true) {
			for (int i=0;i<specificMagicByteArray.length;i++) {
				byte[] currentFilter=specificMagicByteArray[i];
				boolean doesMatchOneMagic=BitcoinUtil.compareMagics(currentFilter,magicNo);
				// correspond to filter? read it!
				if (doesMatchOneMagic==true) {
					readBlock=true;
					break;
				}
			}
			if (readBlock==false) { // skip it
				// Skip block
				this.bin.reset();
				this.bin.skip(blockSize);
			}
		} else {
			readBlock=true;
		}
	}
	// check if it is larger than maxsize, include 8 bytes for the magic and size header
	blockSize=BitcoinUtil.getSize(blockSizeByte)+8;
	if (blockSize==0) throw new BitcoinBlockReadException("Error: Blocksize too small");
	if (blockSize<0) throw new BitcoinBlockReadException("Error: This block size cannot be handled currently (larger then largest number in positive signed int)");
	if (blockSize>this.maxSizeBitcoinBlock) throw new BitcoinBlockReadException("Error: Block size is larger then defined in configuration - Please increase it if this is a valid block");
	// read full block into ByteBuffer
	int blockSizeInt=new Long(blockSize).intValue();
	byte[] fullBlock=new byte[blockSizeInt];
	int fullBlockReadSize=this.bin.read(fullBlock,0,blockSizeInt);
	if (fullBlockReadSize!=blockSize) throw new BitcoinBlockReadException("Error: Could not read full block");
	ByteBuffer result = null;
	if (this.useDirectBuffer==false) {
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
	int currentBlockSize=rawByteBuffer.getInt();
	// version (skip)
	int currentVersion=rawByteBuffer.getInt();
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
*/

public void close() throws IOException {
	this.bin.close();
}

}
