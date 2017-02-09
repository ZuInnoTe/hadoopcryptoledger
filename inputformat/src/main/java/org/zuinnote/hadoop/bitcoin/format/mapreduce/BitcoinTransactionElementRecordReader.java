/**
 * Copyright 2016 Márton Elek
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.zuinnote.hadoop.bitcoin.format.mapreduce;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.NoSuchElementException;

import java.nio.ByteBuffer;


import org.zuinnote.hadoop.bitcoin.format.exception.HadoopCryptoLedgerConfigurationException;
import org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException;

import org.apache.hadoop.io.BytesWritable; 
import org.apache.hadoop.conf.Configuration;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.bitcoin.format.common.*;

public class BitcoinTransactionElementRecordReader extends AbstractBitcoinRecordReader<BytesWritable, BitcoinTransactionElement> {

    private static final Log LOG = LogFactory.getLog(BitcoinTransactionElementRecordReader.class.getName());
private BytesWritable currentKey=new BytesWritable();
private BitcoinTransactionElement currentValue=new BitcoinTransactionElement();

    private int currentTransactionCounterInBlock = 0;

    private int currentInputCounter = 0;

    private int currentOutputCounter = 0;

    private BitcoinBlock currentBitcoinBlock;

    private BitcoinTransaction currentTransaction;

    private byte[] currentBlockHash;

    private byte[] currentTransactionHash;

    public BitcoinTransactionElementRecordReader(Configuration conf) throws HadoopCryptoLedgerConfigurationException {
        super(conf);
    }


/**
*
*  get current key after calling next()
*
* @return key  is a 68 byte array (hashMerkleRoot, prevHashBlock, transActionCounter)
*/
    @Override
    public BytesWritable getCurrentKey() {
        return this.currentKey;
    }


/**
*
*  get current value after calling next()
*
* @return value is a deserialized Java object of class BitcoinTransactionElement
*/
   @Override
    public BitcoinTransactionElement getCurrentValue() {
        return this.currentValue;
    }


    /**
     * Read a next block.
     *
     * @return true if next block is available, false if not
     */
    @Override
    public boolean nextKeyValue() throws IOException {
    // read all the blocks, if necessary a block overlapping a split
            while (getFilePosition() <= getEnd()) { // did we already went beyond the split (remote) or do we have no further data left?
                if (((currentBitcoinBlock == null) || (currentBitcoinBlock.getTransactions().size() == currentTransactionCounterInBlock)) &&(!(processNewBlock()))) {
			return false;
                }
                this.currentValue.setBlockHash(currentBlockHash);
                this.currentValue.setTransactionIdxInBlock(currentTransactionCounterInBlock);
                if (currentTransaction.getListOfInputs().size() > currentInputCounter) {
		    processInputs(this.currentKey, this.currentValue);
                    return true;
                } else if (currentTransaction.getListOfOutputs().size() > currentOutputCounter) {
                    processOutputs(this.currentKey, this.currentValue);
                    return true;
                } else {
                    currentInputCounter = 0;
                    currentOutputCounter = 0;
                    currentTransactionCounterInBlock++;
		    try {
                    	readTransaction();
		     }	catch (NoSuchAlgorithmException e) {
			LOG.error(e);
		    }
                    continue;
                }
            }
    
       
	return false;
    }


 private void processInputs(BytesWritable key, BitcoinTransactionElement value) {
		    value.setType(0);
                    BitcoinTransactionInput input = currentTransaction.getListOfInputs().get(currentInputCounter);
                    value.setIndexInTransaction(input.getPreviousTxOutIndex());
                    value.setAmount(0);
                    value.setTransactionHash(BitcoinUtil.reverseByteArray(input.getPrevTransactionHash()));
                    value.setScript(input.getTxInScript());
                    byte[] keyBytes = createUniqKey(currentTransactionHash, 0, currentInputCounter);
                    key.set(keyBytes, 0, keyBytes.length);
                    currentInputCounter++;
   }

    private void processOutputs(BytesWritable key, BitcoinTransactionElement value) {
		 value.setType(1);
                    BitcoinTransactionOutput output = currentTransaction.getListOfOutputs().get(currentOutputCounter);
                    value.setAmount(output.getValue());
                    value.setIndexInTransaction(currentOutputCounter);
                    value.setTransactionHash(BitcoinUtil.reverseByteArray(currentTransactionHash));
                    value.setScript(output.getTxOutScript());
                    byte[] keyBytes = createUniqKey(currentTransactionHash, 1, currentOutputCounter);
                    key.set(keyBytes, 0, keyBytes.length);

                    //return an output
                    currentOutputCounter++;
    }

    private boolean processNewBlock() throws IOException {
	  try {
                    	currentBitcoinBlock = getBbr().readBlock();
		    } catch (BitcoinBlockReadException e) {
			 LOG.error(e);
		    }
                    if (currentBitcoinBlock == null) {
			return false;
		    }
		    try {
                    	currentBlockHash = BitcoinUtil.getBlockHash(currentBitcoinBlock);
                    	currentTransactionCounterInBlock = 0;
                    	currentInputCounter = 0;
                    	currentOutputCounter = 0;
                    	readTransaction();
	            } catch (IOException|NoSuchAlgorithmException e) {
			LOG.error(e);
		    }
	return true;
    }

    private void readTransaction() throws IOException, NoSuchAlgorithmException {
        if (currentBitcoinBlock.getTransactions().size() > currentTransactionCounterInBlock) {
            currentTransaction = currentBitcoinBlock.getTransactions().get(currentTransactionCounterInBlock);
            currentTransactionHash = BitcoinUtil.getTransactionHash(currentTransaction);
        }
    }

    private byte[] createUniqKey(byte[] transactionHash, int type, int counter) {
        byte[] result = new byte[transactionHash.length + 1 + 4];
        System.arraycopy(transactionHash, 0, result, 0, transactionHash.length);
        System.arraycopy(new byte[]{(byte) type}, 0, result, transactionHash.length, 1);
        System.arraycopy(ByteBuffer.allocate(4).putInt(counter).array(), 0, result, transactionHash.length + 1, 4);
        return result;
    }


}
