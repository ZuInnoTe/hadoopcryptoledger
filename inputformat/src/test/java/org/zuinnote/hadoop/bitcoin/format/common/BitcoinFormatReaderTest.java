/*
 Copyright 2016 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package org.zuinnote.hadoop.bitcoin.format.common;

import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

public class BitcoinFormatReaderTest {

    static final int DEFAULT_BUFFERSIZE = 64 * 1024;
    static final int DEFAULT_MAXSIZE_BITCOINBLOCK = 8 * 1024 * 1024;
    static final byte[][] DEFAULT_MAGIC = {{(byte) 0xF9, (byte) 0xBE, (byte) 0xB4, (byte) 0xD9}};
    private static final byte[][] TESTNET3_MAGIC = {{(byte) 0x0B, (byte) 0x11, (byte) 0x09, (byte) 0x07}};
    private static final byte[][] MULTINET_MAGIC = {{(byte) 0xF9, (byte) 0xBE, (byte) 0xB4, (byte) 0xD9}, {(byte) 0x0B, (byte) 0x11, (byte) 0x09, (byte) 0x07}};

    @Test
    public void parseGenesisBlockAsBitcoinRawBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("genesis.blk", false);
            ByteBuffer genesisByteBuffer = bbr.readRawBlock();
            assertFalse(genesisByteBuffer.isDirect(), "Raw Genesis Block is HeapByteBuffer");
            assertEquals(293, genesisByteBuffer.limit(), "Raw Genesis block has a size of 293 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseVersion1BlockAsBitcoinRawBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("version1.blk", false);
            ByteBuffer version1ByteBuffer = bbr.readRawBlock();
            assertFalse(version1ByteBuffer.isDirect(), "Random Version 1 Raw Block is HeapByteBuffer");
            assertEquals(482, version1ByteBuffer.limit(), "Random Version 1 Raw Block has a size of 482 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseVersion2BlockAsBitcoinRawBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("version2.blk", false);
            ByteBuffer version2ByteBuffer = bbr.readRawBlock();
            assertFalse(version2ByteBuffer.isDirect(), "Random Version 2 Raw Block is HeapByteBuffer");
            assertEquals(191198, version2ByteBuffer.limit(), "Random Version 2 Raw Block has a size of 191.198 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseVersion3BlockAsBitcoinRawBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("version3.blk", false);
            ByteBuffer version3ByteBuffer = bbr.readRawBlock();
            assertFalse(version3ByteBuffer.isDirect(), "Random Version 3 Raw Block is HeapByteBuffer");
            assertEquals(932199, version3ByteBuffer.limit(), "Random Version 3 Raw Block has a size of 932.199 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseVersion4BlockAsBitcoinRawBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("version4.blk", false);
            ByteBuffer version4ByteBuffer = bbr.readRawBlock();
            assertFalse(version4ByteBuffer.isDirect(), "Random Version 4 Raw Block is HeapByteBuffer");
            assertEquals(998039, version4ByteBuffer.limit(), "Random Version 4 Raw Block has a size of 998.039 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseTestNet3GenesisBlockAsBitcoinRawBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("testnet3genesis.blk", false, TESTNET3_MAGIC);
            ByteBuffer genesisByteBuffer = bbr.readRawBlock();
            assertFalse(genesisByteBuffer.isDirect(), "Raw TestNet3 Genesis Block is HeapByteBuffer");
            assertEquals(293, genesisByteBuffer.limit(), "Raw TestNet3 Genesis block has a size of 293 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseTestNet3Version4BlockAsBitcoinRawBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("testnet3version4.blk", false, TESTNET3_MAGIC);
            ByteBuffer version4ByteBuffer = bbr.readRawBlock();
            assertFalse(version4ByteBuffer.isDirect(), "Random TestNet3 Version 4 Raw Block is HeapByteBuffer");
            assertEquals(749041, version4ByteBuffer.limit(), "Random TestNet3 Version 4 Raw Block has a size of 749.041 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseMultiNetAsBitcoinRawBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("multinet.blk", false, MULTINET_MAGIC);
            ByteBuffer firstMultinetByteBuffer = bbr.readRawBlock();
            assertFalse(firstMultinetByteBuffer.isDirect(), "First MultiNetBlock is HeapByteBuffer");
            assertEquals(293, firstMultinetByteBuffer.limit(), "First MultiNetBlock has a size of 293 bytes");
            ByteBuffer secondMultinetByteBuffer = bbr.readRawBlock();
            assertFalse(secondMultinetByteBuffer.isDirect(), "Second MultiNetBlock is HeapByteBuffer");
            assertEquals(191198, secondMultinetByteBuffer.limit(), "Second MultiNetBlock has a size of 191.198 bytes");
            ByteBuffer thirdMultinetByteBuffer = bbr.readRawBlock();
            assertFalse(thirdMultinetByteBuffer.isDirect(), "Third MultiNetBlock is HeapByteBuffer");
            assertEquals(749041, thirdMultinetByteBuffer.limit(), "Third MultiNetBlock has a size of 749.041 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseScriptWitnessBlockAsBitcoinRawBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("scriptwitness.blk", false);
            ByteBuffer scriptwitnessByteBuffer = bbr.readRawBlock();
            assertFalse(scriptwitnessByteBuffer.isDirect(), "Random ScriptWitness Raw Block is HeapByteBuffer");
            assertEquals(999283, scriptwitnessByteBuffer.limit(), "Random ScriptWitness Raw Block has a size of 999283 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseScriptWitness2BlockAsBitcoinRawBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("scriptwitness2.blk", false);
            ByteBuffer scriptwitnessByteBuffer = bbr.readRawBlock();
            assertFalse(scriptwitnessByteBuffer.isDirect(), "Random ScriptWitness Raw Block is HeapByteBuffer");
            assertEquals(1000039, scriptwitnessByteBuffer.limit(), "Random ScriptWitness Raw Block has a size of 1000039 bytes");
            scriptwitnessByteBuffer = bbr.readRawBlock();
            assertFalse(scriptwitnessByteBuffer.isDirect(), "Random ScriptWitness Raw Block is HeapByteBuffer");
            assertEquals(999312, scriptwitnessByteBuffer.limit(), "Random ScriptWitness Raw Block has a size of 999312 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseGenesisBlockAsBitcoinRawBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("genesis.blk", true);
            ByteBuffer genesisByteBuffer = bbr.readRawBlock();
            assertTrue(genesisByteBuffer.isDirect(), "Raw Genesis Block is DirectByteBuffer");
            assertEquals(293, genesisByteBuffer.limit(), "Raw Genesis Block has a size of 293 bytes");
        } finally {
            if (bbr != null) {
                bbr.close();
            }
        }
    }

    @Test
    public void parseGenesisBlockTestTime() throws IOException, BitcoinBlockReadException, ParseException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = genesisBlockReader(true);
            BitcoinBlock genesisBlock = bbr.readBlock();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd h:m:s");
            Date genesisDate = format.parse ( "2009-01-03 18:15:05" );
            Date blockDate = new java.util.Date(genesisBlock.getTime()*1000L);
            assertDateFieldsEqual(genesisDate, blockDate);
        } finally {
            if (bbr != null) {
                bbr.close();
            }
        }
    }

    @Test
    public void parseVersion1BlockAsBitcoinRawBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("version1.blk", true);
            ByteBuffer version1ByteBuffer = bbr.readRawBlock();
            assertTrue(version1ByteBuffer.isDirect(), "Random Version 1 Raw Block is DirectByteBuffer");
            assertEquals(482, version1ByteBuffer.limit(), "Random Version 1 Raw Block has a size of 482 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseVersion2BlockAsBitcoinRawBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("version2.blk", true);
            ByteBuffer version2ByteBuffer = bbr.readRawBlock();
            assertTrue(version2ByteBuffer.isDirect(), "Random Version 2 Raw Block is DirectByteBuffer");
            assertEquals(191198, version2ByteBuffer.limit(), "Random Version 2 Raw Block has a size of 191.198 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseVersion3BlockAsBitcoinRawBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("version3.blk", true);
            ByteBuffer version3ByteBuffer = bbr.readRawBlock();
            assertTrue(version3ByteBuffer.isDirect(), "Random Version 3 Raw Block is DirectByteBuffer");
            assertEquals(932199, version3ByteBuffer.limit(), "Random Version 3 Raw Block has a size of 932.199 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseVersion4BlockAsBitcoinRawBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("version4.blk", true);
            ByteBuffer version4ByteBuffer = bbr.readRawBlock();
            assertTrue(version4ByteBuffer.isDirect(), "Random Version 4 Raw Block is DirectByteBuffer");
            assertEquals(998039, version4ByteBuffer.limit(), "Random Version 4 Raw Block has a size of 998.039 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseTestNet3GenesisBlockAsBitcoinRawBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("testnet3genesis.blk", true, TESTNET3_MAGIC);
            ByteBuffer genesisByteBuffer = bbr.readRawBlock();
            assertTrue(genesisByteBuffer.isDirect(), "Raw TestNet3 Genesis Block is DirectByteBuffer");
            assertEquals(293, genesisByteBuffer.limit(), "Raw TestNet3 Genesis block has a size of 293 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseTestNet3Version4BlockAsBitcoinRawBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("testnet3version4.blk", true, TESTNET3_MAGIC);
            ByteBuffer version4ByteBuffer = bbr.readRawBlock();
            assertTrue(version4ByteBuffer.isDirect(), "Random TestNet3 Version 4 Raw Block is DirectByteBuffer");
            assertEquals(749041, version4ByteBuffer.limit(), "Random TestNet3  Version 4 Raw Block has a size of 749.041 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseMultiNetAsBitcoinRawBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("multinet.blk", true, MULTINET_MAGIC);
            ByteBuffer firstMultinetByteBuffer = bbr.readRawBlock();
            assertTrue(firstMultinetByteBuffer.isDirect(), "First MultiNetBlock is DirectByteBuffer");
            assertEquals(293, firstMultinetByteBuffer.limit(), "First MultiNetBlock has a size of 293 bytes");
            ByteBuffer secondMultinetByteBuffer = bbr.readRawBlock();
            assertTrue(secondMultinetByteBuffer.isDirect(), "Second MultiNetBlock is DirectByteBuffer");
            assertEquals(191198, secondMultinetByteBuffer.limit(), "Second MultiNetBlock has a size of 191.198 bytes");
            ByteBuffer thirdMultinetByteBuffer = bbr.readRawBlock();
            assertTrue(thirdMultinetByteBuffer.isDirect(), "Third MultiNetBlock is DirectByteBuffer");
            assertEquals(749041, thirdMultinetByteBuffer.limit(), "Third MultiNetBlock has a size of 749.041 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseScriptWitnessBlockAsBitcoinRawBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("scriptwitness.blk", true);
            ByteBuffer scriptwitnessByteBuffer = bbr.readRawBlock();
            assertTrue(scriptwitnessByteBuffer.isDirect(), "Random ScriptWitness Raw Block is DirectByteBuffer");
            assertEquals(999283, scriptwitnessByteBuffer.limit(), "Random ScriptWitness Raw Block has a size of 999283 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseScriptWitness2BlockAsBitcoinRawBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("scriptwitness2.blk", true);
            ByteBuffer scriptwitnessByteBuffer = bbr.readRawBlock();
            assertTrue(scriptwitnessByteBuffer.isDirect(), "Random ScriptWitness Raw Block is DirectByteBuffer");
            assertEquals(1000039, scriptwitnessByteBuffer.limit(), "Random ScriptWitness Raw Block has a size of 1000039 bytes");
            scriptwitnessByteBuffer = bbr.readRawBlock();
            assertTrue(scriptwitnessByteBuffer.isDirect(), "Random ScriptWitness Raw Block is DirectByteBuffer");
            assertEquals(999312, scriptwitnessByteBuffer.limit(), "Random ScriptWitness Raw Block has a size of 999312 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseGenesisBlockAsBitcoinBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("genesis.blk", false);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(1, theBitcoinBlock.getTransactions().size(), "Genesis Block must contain exactly one transaction");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "Genesis Block must contain exactly one transaction with one input");
            assertEquals(77, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "Genesis Block must contain exactly one transaction with one input and script length 77");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "Genesis Block must contain exactly one transaction with one output");
            assertEquals(BigInteger.valueOf(5000000000L), theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getValue(), "Value must be BigInteger corresponding to 5000000000L");
            assertEquals(67, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "Genesis Block must contain exactly one transaction with one output and script length 67");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseTestNet3GenesisBlockAsBitcoinBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("testnet3genesis.blk", false, TESTNET3_MAGIC);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(1, theBitcoinBlock.getTransactions().size(), "TestNet3 Genesis Block must contain exactly one transaction");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "TestNet3 Genesis Block must contain exactly one transaction with one input");
            assertEquals(77, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "TestNet3 Genesis Block must contain exactly one transaction with one input and script length 77");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "TestNet3 Genesis Block must contain exactly one transaction with one output");
            assertEquals(67, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "TestNet3 Genesis Block must contain exactly one transaction with one output and script length 67");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseVersion1BlockAsBitcoinBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("version1.blk", false);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(2, theBitcoinBlock.getTransactions().size(), "Random Version 1 Block must contain exactly two transactions");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "Random Version 1 Block must contain exactly two transactions of which the first has one input");
            assertEquals(8, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "Random Version 1 Block must contain exactly two transactions of which the first has one input and script length 8");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "Random Version 1 Block must contain exactly two transactions of which the first has one output");
            assertEquals(67, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "Random Version 1 Block must contain exactly two transactions of which the first has one output and script length 67");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseVersion2BlockAsBitcoinBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("version2.blk", false, DEFAULT_MAGIC);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(343, theBitcoinBlock.getTransactions().size(), "Random Version 2 Block must contain exactly 343 transactions");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "Random Version 2 Block must contain exactly 343 transactions of which the first has one input");
            assertEquals(40, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "Random Version 2 Block must contain exactly 343 transactions of which the first has one input and script length 40");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "Random Version 2 Block must contain exactly 343 transactions of which the first has one output");
            assertEquals(25, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "Random Version 2 Block must contain exactly 343 transactions of which the first has one output and script length 25");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseVersion3BlockAsBitcoinBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("version3.blk", false);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(1645, theBitcoinBlock.getTransactions().size(), "Random Version 3 Block must contain exactly 1645 transactions");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "Random Version 3 Block must contain exactly 1645 transactions of which the first has one input");
            assertEquals(49, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "Random Version 3 Block must contain exactly 1645 transactions of which the first has one input and script length 49");
            assertEquals(2, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "Random Version 3 Block must contain exactly 1645 transactions of which the first has two outputs");
            assertEquals(25, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "Random Version 3 Block must contain exactly 1645 transactions of which the first has two output and the first output script length 25");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseVersion4BlockAsBitcoinBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("version4.blk", false);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(936, theBitcoinBlock.getTransactions().size(), "Random Version 4 Block must contain exactly 936 transactions");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "Random Version 4 Block must contain exactly 936 transactions of which the first has one input");
            assertEquals(4, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "Random Version 4 Block must contain exactly 936 transactions of which the first has one input and script length 4");
            assertEquals(2, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "Random Version 4 Block must contain exactly 936 transactions of which the first has two outputs");
            assertEquals(25, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "Random Version 4 Block must contain exactly 936 transactions of which the first has two output and the first output script length 25");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseTestNet3Version4BlockAsBitcoinBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("testnet3version4.blk", false, TESTNET3_MAGIC);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(3299, theBitcoinBlock.getTransactions().size(), "Random TestNet3 Version 4 Block must contain exactly 3299 transactions");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "Random TestNet3 Version 4 Block must contain exactly 3299 transactions of which the first has one input");
            assertEquals(35, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "Random TestNet3 Version 4 Block must contain exactly 3299 transactions of which the first has one input and script length 35");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "Random TestNet3 Version 4 Block must contain exactly 3299 transactions of which the first has one outputs");
            assertEquals(25, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "Random TestNet3 Version 4 Block must contain exactly 3299 transactions of which the first has one output and the first output script length 25");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseMultiNetBlockAsBitcoinBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("multinet.blk", false, MULTINET_MAGIC);
            BitcoinBlock firstBitcoinBlock = bbr.readBlock();
            assertEquals(1, firstBitcoinBlock.getTransactions().size(), "First MultiNet Block must contain exactly one transaction");
            assertEquals(1, firstBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "First MultiNet Block must contain exactly one transaction with one input");
            assertEquals(77, firstBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "First MultiNet Block must contain exactly one transaction with one input and script length 77");
            assertEquals(1, firstBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "First MultiNet Block must contain exactly one transaction with one output");
            assertEquals(67, firstBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "First MultiNet Block must contain exactly one transaction with one output and script length 67");
            BitcoinBlock secondBitcoinBlock = bbr.readBlock();
            assertEquals(343, secondBitcoinBlock.getTransactions().size(), "Second MultiNet Block must contain exactly 343 transactions");
            assertEquals(1, secondBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "Second MultiNet Block must contain exactly 343 transactions of which the first has one input");
            assertEquals(40, secondBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "Second MultiNet Block must contain exactly 343 transactions of which the first has one input and script length 40");
            assertEquals(1, secondBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "Second MultiNet Block must contain exactly 343 transactions of which the first has one output");
            assertEquals(25, secondBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "Second MultiNet Block must contain exactly 343 transactions of which the first has one output and script length 25");
            BitcoinBlock thirdBitcoinBlock = bbr.readBlock();
            assertEquals(3299, thirdBitcoinBlock.getTransactions().size(), "Third MultiNet Block must contain exactly 3299 transactions");
            assertEquals(1, thirdBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "Third MultiNet Block must contain exactly 3299 transactions of which the first has one input");
            assertEquals(35, thirdBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "Third MultiNet Block must contain exactly 3299 transactions of which the first has one input and script length 35");
            assertEquals(1, thirdBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "Third MultiNet Block must contain exactly 3299 transactions of which the first has one outputs");
            assertEquals(25, thirdBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "Third MultiNet Block must contain exactly 3299 transactions of which the first has one output and the first output script length 25");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseScriptWitnessBlockAsBitcoinBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("scriptwitness.blk", false);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(470, theBitcoinBlock.getTransactions().size(), "Random ScriptWitness Block must contain exactly 470 transactions");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseScriptWitness2BlockAsBitcoinBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("scriptwitness2.blk", false);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(2191, theBitcoinBlock.getTransactions().size(), "First random ScriptWitness Block must contain exactly 2191 transactions");
            theBitcoinBlock = bbr.readBlock();
            assertEquals(2508, theBitcoinBlock.getTransactions().size(), "Second random ScriptWitness Block must contain exactly 2508 transactions");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseGenesisBlockAsBitcoinBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("genesis.blk", true);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(1, theBitcoinBlock.getTransactions().size(), "Genesis Block must contain exactly one transaction");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "Genesis Block must contain exactly one transaction with one input");
            assertEquals(77, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "Genesis Block must contain exactly one transaction with one input and script length 77");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "Genesis Block must contain exactly one transaction with one output");
            assertEquals(67, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "Genesis Block must contain exactly one transaction with one output and script length 67");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseTestNet3GenesisBlockAsBitcoinBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("testnet3genesis.blk", true, TESTNET3_MAGIC);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(1, theBitcoinBlock.getTransactions().size(), "TestNet3 Genesis Block must contain exactly one transaction");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "TestNet3 Genesis Block must contain exactly one transaction with one input");
            assertEquals(77, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "TestNet3 Genesis Block must contain exactly one transaction with one input and script length 77");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "TestNet3 Genesis Block must contain exactly one transaction with one output");
            assertEquals(67, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "TestNet3 Genesis Block must contain exactly one transaction with one output and script length 67");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }


    @Test
    public void parseVersion1BlockAsBitcoinBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("version1.blk", true);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(2, theBitcoinBlock.getTransactions().size(), "Random Version 1 Block must contain exactly two transactions");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "Random Version 1 Block must contain exactly two transactions of which the first has one input");
            assertEquals(8, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "Random Version 1 Block must contain exactly two transactions of which the first has one input and script length 8");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "Random Version 1 Block must contain exactly two transactions of which the first has one output");
            assertEquals(67, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "Random Version 1 Block must contain exactly two transactions of which the first has one output and script length 67");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseVersion2BlockAsBitcoinBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("version2.blk", true);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(343, theBitcoinBlock.getTransactions().size(), "Random Version 2 Block must contain exactly 343 transactions");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "Random Version 2 Block must contain exactly 343 transactions of which the first has one input");
            assertEquals(40, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "Random Version 2 Block must contain exactly 343 transactions of which the first has one input and script length 40");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "Random Version 2 Block must contain exactly 343 transactions of which the first has one output");
            assertEquals(25, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "Random Version 2 Block must contain exactly 343 transactions of which the first has one output and script length 25");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseVersion3BlockAsBitcoinBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("version3.blk", true);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(1645, theBitcoinBlock.getTransactions().size(), "Random Version 3 Block must contain exactly 1645 transactions");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "Random Version 3 Block must contain exactly 1645 transactions of which the first has one input");
            assertEquals(49, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "Random Version 3 Block must contain exactly 1645 transactions of which the first has one input and script length 49");
            assertEquals(2, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "Random Version 3 Block must contain exactly 1645 transactions of which the first has two outputs");
            assertEquals(25, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "Random Version 3 Block must contain exactly 1645 transactions of which the first has two output and the first output script length 25");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseVersion4BlockAsBitcoinBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("version4.blk", true);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(936, theBitcoinBlock.getTransactions().size(), "Random Version 4 Block must contain exactly 936 transactions");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "Random Version 4 Block must contain exactly 936 transactions of which the first has one input");
            assertEquals(4, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "Random Version 4 Block must contain exactly 936 transactions of which the first has one input and script length 4");
            assertEquals(2, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "Random Version 4 Block must contain exactly 936 transactions of which the first has two outputs");
            assertEquals(25, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "Random Version 4 Block must contain exactly 936 transactions of which the first has two output and the first output script length 25");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseTestNet3Version4BlockAsBitcoinBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("testnet3version4.blk", true, TESTNET3_MAGIC);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(3299, theBitcoinBlock.getTransactions().size(), "Random TestNet3 Version 4 Block must contain exactly 3299 transactions");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "Random TestNet3 Version 4 Block must contain exactly 3299 transactions of which the first has one input");
            assertEquals(35, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "Random TestNet3 Version 4 Block must contain exactly 3299 transactions of which the first has one input and script length 35");
            assertEquals(1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "Random TestNet3 Version 4 Block must contain exactly 3299 transactions of which the first has one outputs");
            assertEquals(25, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "Random TestNet3 Version 4 Block must contain exactly 3299 transactions of which the first has one output and the first output script length 25");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseMultiNetBlockAsBitcoinBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("multinet.blk", true, MULTINET_MAGIC);
            BitcoinBlock firstBitcoinBlock = bbr.readBlock();
            assertEquals(1, firstBitcoinBlock.getTransactions().size(), "First MultiNet Block must contain exactly one transaction");
            assertEquals(1, firstBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "First MultiNet Block must contain exactly one transaction with one input");
            assertEquals(77, firstBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "First MultiNet Block must contain exactly one transaction with one input and script length 77");
            assertEquals(1, firstBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "First MultiNet Block must contain exactly one transaction with one output");
            assertEquals(67, firstBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "First MultiNet Block must contain exactly one transaction with one output and script length 67");
            BitcoinBlock secondBitcoinBlock = bbr.readBlock();
            assertEquals(343, secondBitcoinBlock.getTransactions().size(), "Second MultiNet Block must contain exactly 343 transactions");
            assertEquals(1, secondBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "Second MultiNet Block must contain exactly 343 transactions of which the first has one input");
            assertEquals(40, secondBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "Second MultiNet Block must contain exactly 343 transactions of which the first has one input and script length 40");
            assertEquals(1, secondBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "Second MultiNet Block must contain exactly 343 transactions of which the first has one output");
            assertEquals(25, secondBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "Second MultiNet Block must contain exactly 343 transactions of which the first has one output and script length 25");
            BitcoinBlock thirdBitcoinBlock = bbr.readBlock();
            assertEquals(3299, thirdBitcoinBlock.getTransactions().size(), "Third MultiNet Block must contain exactly 3299 transactions");
            assertEquals(1, thirdBitcoinBlock.getTransactions().get(0).getListOfInputs().size(), "Third MultiNet Block must contain exactly 3299 transactions of which the first has one input");
            assertEquals(35, thirdBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length, "Third MultiNet Block must contain exactly 3299 transactions of which the first has one input and script length 35");
            assertEquals(1, thirdBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(), "Third MultiNet Block must contain exactly 3299 transactions of which the first has one outputs");
            assertEquals(25, thirdBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length, "Third MultiNet Block must contain exactly 3299 transactions of which the first has one output and the first output script length 25");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseScriptWitnessBlockAsBitcoinBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("scriptwitness.blk", true);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(470, theBitcoinBlock.getTransactions().size(), "Random ScriptWitness Block must contain exactly 470 transactions");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void parseScriptWitness2BlockAsBitcoinBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("scriptwitness2.blk", true);
            BitcoinBlock theBitcoinBlock = bbr.readBlock();
            assertEquals(2191, theBitcoinBlock.getTransactions().size(), "First random ScriptWitness Block must contain exactly 2191 transactions");
            theBitcoinBlock = bbr.readBlock();
            assertEquals(2508, theBitcoinBlock.getTransactions().size(), "Second random ScriptWitness Block must contain exactly 2508 transactions");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void seekBlockStartHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("reqseekversion1.blk", false);
            bbr.seekBlockStart();
            ByteBuffer version1ByteBuffer = bbr.readRawBlock();
            assertFalse(version1ByteBuffer.isDirect(), "Random Version 1 Raw Block (requiring seek) is HeapByteBuffer");
            assertEquals(482, version1ByteBuffer.limit(), "Random Version 1 Raw Block (requiring seek) has a size of 482 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void seekBlockStartDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("reqseekversion1.blk", true);
            bbr.seekBlockStart();
            ByteBuffer version1ByteBuffer = bbr.readRawBlock();
            assertTrue(version1ByteBuffer.isDirect(), "Random Version 1 Raw Block (requiring seek) is DirectByteBuffer");
            assertEquals(482, version1ByteBuffer.limit(), "Random Version 1 Raw Block (requiring seek) has a size of 482 bytes");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void getKeyFromRawBlockHeap() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("genesis.blk", false);
            ByteBuffer genesisByteBuffer = bbr.readRawBlock();
            assertFalse(genesisByteBuffer.isDirect(), "Raw Genesis Block is HeapByteBuffer");
            byte[] key = bbr.getKeyFromRawBlock(genesisByteBuffer);
            assertEquals(64, key.length, "Raw Genesis Block Key should have a size of 64 bytes");
            byte[] comparatorKey = new byte[]{(byte) 0x3B, (byte) 0xA3, (byte) 0xED, (byte) 0xFD, (byte) 0x7A, (byte) 0x7B, (byte) 0x12, (byte) 0xB2, (byte) 0x7A, (byte) 0xC7, (byte) 0x2C, (byte) 0x3E, (byte) 0x67, (byte) 0x76, (byte) 0x8F, (byte) 0x61, (byte) 0x7F, (byte) 0xC8, (byte) 0x1B, (byte) 0xC3, (byte) 0x88, (byte) 0x8A, (byte) 0x51, (byte) 0x32, (byte) 0x3A, (byte) 0x9F, (byte) 0xB8, (byte) 0xAA, (byte) 0x4B, (byte) 0x1E, (byte) 0x5E, (byte) 0x4A, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00};
            assertArrayEquals(comparatorKey, key, "Raw Genesis Block Key is equivalent to comparator key");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    @Test
    public void getKeyFromRawBlockDirect() throws IOException, BitcoinBlockReadException {
        BitcoinBlockReader bbr = null;
        try {
            bbr = assertBlockAvailable("genesis.blk", true);
            ByteBuffer genesisByteBuffer = bbr.readRawBlock();
            assertTrue(genesisByteBuffer.isDirect(), "Raw Genesis Block is DirectByteBuffer");
            byte[] key = bbr.getKeyFromRawBlock(genesisByteBuffer);
            assertEquals(64, key.length, "Raw Genesis Block Key should have a size of 64 bytes");
            byte[] comparatorKey = new byte[]{(byte) 0x3B, (byte) 0xA3, (byte) 0xED, (byte) 0xFD, (byte) 0x7A, (byte) 0x7B, (byte) 0x12, (byte) 0xB2, (byte) 0x7A, (byte) 0xC7, (byte) 0x2C, (byte) 0x3E, (byte) 0x67, (byte) 0x76, (byte) 0x8F, (byte) 0x61, (byte) 0x7F, (byte) 0xC8, (byte) 0x1B, (byte) 0xC3, (byte) 0x88, (byte) 0x8A, (byte) 0x51, (byte) 0x32, (byte) 0x3A, (byte) 0x9F, (byte) 0xB8, (byte) 0xAA, (byte) 0x4B, (byte) 0x1E, (byte) 0x5E, (byte) 0x4A, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00};
            assertArrayEquals(comparatorKey, key, "Raw Genesis Block Key is equivalent to comparator key");
        } finally {
            if (bbr != null)
                bbr.close();
        }
    }

    public String getFullFilename(String fileName) {
        return Objects.requireNonNull(getClass().getClassLoader().getResource("testdata/" + fileName)).getFile();
    }

    public BitcoinBlockReader assertBlockAvailable(String fileName, boolean direct) throws IOException {
        return assertBlockAvailable(fileName, direct, DEFAULT_MAGIC);
    }

    public BitcoinBlockReader assertBlockAvailable(String fileName, boolean direct, byte[][] magic) throws IOException {
        assertTestFileAvailable(fileName);
        File file = new File(getFullFilename(fileName));
        FileInputStream fin = new FileInputStream(file);
        return new BitcoinBlockReader(fin, DEFAULT_MAXSIZE_BITCOINBLOCK, DEFAULT_BUFFERSIZE, magic, direct);
    }

    public BitcoinBlockReader genesisBlockReader(boolean direct) throws IOException {
        return assertBlockAvailable("genesis.blk", direct);
    }

    public void assertTestFileAvailable(String fileName) {
        String fullFilename = getFullFilename(fileName);
        File file = new File(fullFilename);
        assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
        assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
    }

    public void assertDateFieldEqual(Calendar expected, Calendar actual, int field) {
        assertEquals(expected.get(field), actual.get(field));
    }

    public void assertDateFieldsEqual(Date expected, Date actual) {
        Calendar x = new GregorianCalendar();
        x.setTime(expected);
        Calendar y = new GregorianCalendar();
        y.setTime(actual);
        Integer[] fields = new Integer[]
                {Calendar.YEAR, Calendar.MONTH, Calendar.DAY_OF_MONTH, Calendar.HOUR_OF_DAY, Calendar.MINUTE};
        for(int field : fields) {
            assertDateFieldEqual(x, y, field);
        }
    }

}
