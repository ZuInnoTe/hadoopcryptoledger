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

package org.zuinnote.hadoop.bitcoin.format;
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
    public BytesWritable getCurrentKey() {
        return this.currentKey;
    }


/**
*
*  get current value after calling next()
*
* @return value is a deserialized Java object of class BitcoinTransactionElement
*/
    public BitcoinTransactionElement getCurrentValue() {
        return this.currentValue;
    }


    /**
     * Read a next block.
     *
     * @return true if next block is available, false if not
     */
    public boolean nextKeyValue() throws IOException {
        // read all the blocks, if necessary a block overlapping a split´
        try {
            while (getFilePosition() <= getEnd()) { // did we already went beyond the split (remote) or do we have no further data left?
                if ((currentBitcoinBlock == null) || (currentBitcoinBlock.getTransactions().size() == currentTransactionCounterInBlock)) {
                    currentBitcoinBlock = getBbr().readBlock();
                    if (currentBitcoinBlock == null) return false;
                    currentBlockHash = BitcoinUtil.getBlockHash(currentBitcoinBlock);
                    currentTransactionCounterInBlock = 0;
                    currentInputCounter = 0;
                    currentOutputCounter = 0;
                    readTransaction();
                }


                this.currentValue.setBlockHash(currentBlockHash);
                this.currentValue.setTransactionIdxInBlock(currentTransactionCounterInBlock);
                if (currentTransaction.getListOfInputs().size() > currentInputCounter) {
                    this.currentValue.setType(0);
                    BitcoinTransactionInput input = currentTransaction.getListOfInputs().get(currentInputCounter);
                    this.currentValue.setIndexInTransaction(input.getPreviousTxOutIndex());
                    this.currentValue.setAmount(0);
                    this.currentValue.setTransactionHash(BitcoinUtil.reverseByteArray(input.getPrevTransactionHash()));
                    this.currentValue.setScript(input.getTxInScript());
                    byte[] keyBytes = createUniqKey(currentTransactionHash, 0, currentInputCounter);
                    this.currentKey.set(keyBytes, 0, keyBytes.length);
                    currentInputCounter++;
                    return true;
                } else if (currentTransaction.getListOfOutputs().size() > currentOutputCounter) {
                    this.currentValue.setType(1);
                    BitcoinTransactionOutput output = currentTransaction.getListOfOutputs().get(currentOutputCounter);
                    this.currentValue.setAmount(output.getValue());
                    this.currentValue.setIndexInTransaction(currentOutputCounter);
                    this.currentValue.setTransactionHash(BitcoinUtil.reverseByteArray(currentTransactionHash));
                    this.currentValue.setScript(output.getTxOutScript());
                    byte[] keyBytes = createUniqKey(currentTransactionHash, 1, currentOutputCounter);
                    this.currentKey.set(keyBytes, 0, keyBytes.length);

                    //return an output
                    currentOutputCounter++;
                    return true;
                } else {
                    currentInputCounter = 0;
                    currentOutputCounter = 0;
                    currentTransactionCounterInBlock++;
                    readTransaction();
                    continue;
                }
            }
        } catch (NoSuchElementException e) {
            LOG.error(e);
        } catch (BitcoinBlockReadException e) {
            LOG.error(e);
        } catch (NoSuchAlgorithmException e) {
            LOG.error(e);
        }
    	return false;
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
