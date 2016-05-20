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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.nio.ByteBuffer;

import org.zuinnote.hadoop.bitcoin.format.BitcoinBlockReader;
import org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException;


public class BitcoinFormatReaderTest {
private static final int DEFAULT_BUFFERSIZE=64*1024;
private static final int DEFAULT_MAXSIZE_BITCOINBLOCK=1 * 1024 * 1024;
private static final byte[][] DEFAULT_MAGIC = {{(byte)0xF9,(byte)0xBE,(byte)0xB4,(byte)0xD9}};

 @Test
  public void checkTestDataGenesisBlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="genesis.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameGenesis);
	File file = new File(fileNameGenesis);
	assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
	assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
  }


 @Test
  public void checkTestDataVersion1BlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version1.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameGenesis);
	File file = new File(fileNameGenesis);
	assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
	assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
  }

 @Test
  public void checkTestDataVersion2BlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version2.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameGenesis);
	File file = new File(fileNameGenesis);
	assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
	assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
  }


 @Test
  public void checkTestDataVersion3BlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version3.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameGenesis);
	File file = new File(fileNameGenesis);
	assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
	assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
  }

 @Test
  public void checkTestDataVersion4BlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version4.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameGenesis);
	File file = new File(fileNameGenesis);
	assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
	assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
  }

 @Test
  public void checkTestDataVersion1SeekBlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="reqseekversion1.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameGenesis);
	File file = new File(fileNameGenesis);
	assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
	assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
  }


  @Test
  public void parseGenesisBlockAsBitcoinRawBlockHeap() throws FileNotFoundException, IOException, BitcoinBlockReadException {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="genesis.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameGenesis);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		ByteBuffer genesisByteBuffer = bbr.readRawBlock();
		assertFalse("Raw Genesis Block is HeapByteBuffer", genesisByteBuffer.isDirect());
		assertEquals("Raw Genesis block has a size of 293 bytes", 293, genesisByteBuffer.limit());
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseVersion1BlockAsBitcoinRawBlockHeap()  throws FileNotFoundException, IOException, BitcoinBlockReadException {
    ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version1.blk";
	String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameBlock);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		ByteBuffer version1ByteBuffer = bbr.readRawBlock();
		assertFalse("Random Version 1 Raw Block is HeapByteBuffer", version1ByteBuffer.isDirect());
		assertEquals("Random Version 1 Raw Block has a size of 482 bytes", 482, version1ByteBuffer.limit());
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseVersion2BlockAsBitcoinRawBlockHeap() throws FileNotFoundException, IOException, BitcoinBlockReadException {
    ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version2.blk";
	String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameBlock);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		ByteBuffer version2ByteBuffer = bbr.readRawBlock();
		assertFalse("Random Version 2 Raw Block is HeapByteBuffer", version2ByteBuffer.isDirect());
		assertEquals("Random Version 2 Raw Block has a size of 191.198 bytes", 191198, version2ByteBuffer.limit());
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }


  @Test
  public void parseVersion3BlockAsBitcoinRawBlockHeap() throws FileNotFoundException, IOException, BitcoinBlockReadException {
    ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version3.blk";
	String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameBlock);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		ByteBuffer version3ByteBuffer = bbr.readRawBlock();
		assertFalse("Random Version 3 Raw Block is HeapByteBuffer", version3ByteBuffer.isDirect());
		assertEquals("Random Version 3 Raw Block has a size of 932.199 bytes", 932199, version3ByteBuffer.limit());
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseVersion4BlockAsBitcoinRawBlockHeap()  throws FileNotFoundException, IOException, BitcoinBlockReadException {
    ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version4.blk";
	String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameBlock);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		ByteBuffer version4ByteBuffer = bbr.readRawBlock();
		assertFalse("Random Version 4 Raw Block is HeapByteBuffer", version4ByteBuffer.isDirect());
		assertEquals("Random Version 4 Raw Block has a size of 998.039 bytes", 998039, version4ByteBuffer.limit());
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }


  @Test
  public void parseGenesisBlockAsBitcoinRawBlockDirect() throws FileNotFoundException, IOException, BitcoinBlockReadException {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="genesis.blk";
	String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameBlock);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		ByteBuffer genesisByteBuffer = bbr.readRawBlock();
		assertTrue("Raw Genesis Block is DirectByteBuffer", genesisByteBuffer.isDirect());
		assertEquals("Raw Genesis Block has a size of 293 bytes", 293, genesisByteBuffer.limit());
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseVersion1BlockAsBitcoinRawBlockDirect() throws FileNotFoundException, IOException, BitcoinBlockReadException {
    ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version1.blk";
	String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameBlock);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		ByteBuffer version1ByteBuffer = bbr.readRawBlock();
		assertTrue("Random Version 1 Raw Block is DirectByteBuffer", version1ByteBuffer.isDirect());
		assertEquals("Random Version 1 Raw Block has a size of 482 bytes", 482, version1ByteBuffer.limit());
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseVersion2BlockAsBitcoinRawBlockDirect() throws FileNotFoundException, IOException, BitcoinBlockReadException {
    ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version2.blk";
	String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameBlock);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		ByteBuffer version2ByteBuffer = bbr.readRawBlock();
		assertTrue("Random Version 2 Raw Block is DirectByteBuffer", version2ByteBuffer.isDirect());
		assertEquals("Random Version 2 Raw Block has a size of 191.198 bytes", 191198, version2ByteBuffer.limit());
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseVersion3BlockAsBitcoinRawBlockDirect()  throws FileNotFoundException, IOException, BitcoinBlockReadException {
    ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version3.blk";
	String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameBlock);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		ByteBuffer version3ByteBuffer = bbr.readRawBlock();
		assertTrue("Random Version 3 Raw Block is DirectByteBuffer", version3ByteBuffer.isDirect());
		assertEquals("Random Version 3 Raw Block has a size of 932.199 bytes", 932199, version3ByteBuffer.limit());
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseVersion4BlockAsBitcoinRawBlockDirect()  throws FileNotFoundException, IOException, BitcoinBlockReadException {
    ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version4.blk";
	String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameBlock);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		ByteBuffer version4ByteBuffer = bbr.readRawBlock();
		assertTrue("Random Version 4 Raw Block is DirectByteBuffer", version4ByteBuffer.isDirect());
		assertEquals("Random Version 4 Raw Block has a size of 998.039 bytes", 998039, version4ByteBuffer.limit());
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseGenesisBlockAsBitcoinBlockHeap() throws FileNotFoundException, IOException, BitcoinBlockReadException {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="genesis.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals("Genesis Block must contain exactly one transaction", 1, theBitcoinBlock.getTransactions().length);
		assertEquals("Genesis Block must contain exactly one transaction with one input", 1, theBitcoinBlock.getTransactions()[0].getListOfInputs().length);
		assertEquals("Genesis Block must contain exactly one transaction with one input and script length 77", 77, theBitcoinBlock.getTransactions()[0].getListOfInputs()[0].getTxInScript().length);
		assertEquals("Genesis Block must contain exactly one transaction with one output", 1, theBitcoinBlock.getTransactions()[0].getListOfOutputs().length);
		assertEquals("Genesis Block must contain exactly one transaction with one output and script length 67", 67, theBitcoinBlock.getTransactions()[0].getListOfOutputs()[0].getTxOutScript().length);
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseVersion1BlockAsBitcoinBlockHeap() throws FileNotFoundException, IOException, BitcoinBlockReadException {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version1.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals("Random Version 1 Block must contain exactly two transactions", 2, theBitcoinBlock.getTransactions().length);
		assertEquals("Random Version 1 Block must contain exactly two transactions of which the first has one input", 1, theBitcoinBlock.getTransactions()[0].getListOfInputs().length);
		assertEquals("Random Version 1 Block must contain exactly two transactions of which the first has one input and script length 8", 8, theBitcoinBlock.getTransactions()[0].getListOfInputs()[0].getTxInScript().length);
		assertEquals("Random Version 1 Block must contain exactly two transactions of which the first has one output", 1, theBitcoinBlock.getTransactions()[0].getListOfOutputs().length);
		assertEquals("Random Version 1 Block must contain exactly two transactions of which the first has one output and script length 67", 67, theBitcoinBlock.getTransactions()[0].getListOfOutputs()[0].getTxOutScript().length);
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseVersion2BlockAsBitcoinBlockHeap() throws FileNotFoundException, IOException, BitcoinBlockReadException {
    	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version2.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals("Random Version 2 Block must contain exactly 343 transactions", 343, theBitcoinBlock.getTransactions().length);
		assertEquals("Random Version 2 Block must contain exactly 343 transactions of which the first has one input", 1, theBitcoinBlock.getTransactions()[0].getListOfInputs().length);
		assertEquals("Random Version 2 Block must contain exactly 343 transactions of which the first has one input and script length 40", 40, theBitcoinBlock.getTransactions()[0].getListOfInputs()[0].getTxInScript().length);
		assertEquals("Random Version 2 Block must contain exactly 343 transactions of which the first has one output", 1, theBitcoinBlock.getTransactions()[0].getListOfOutputs().length);
		assertEquals("Random Version 2 Block must contain exactly 343 transactions of which the first has one output and script length 25", 25, theBitcoinBlock.getTransactions()[0].getListOfOutputs()[0].getTxOutScript().length);
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseVersion3BlockAsBitcoinBlockHeap() throws FileNotFoundException, IOException, BitcoinBlockReadException {
        ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version3.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals("Random Version 3 Block must contain exactly 1645 transactions", 1645, theBitcoinBlock.getTransactions().length);
		assertEquals("Random Version 3 Block must contain exactly 1645 transactions of which the first has one input", 1, theBitcoinBlock.getTransactions()[0].getListOfInputs().length);
		assertEquals("Random Version 3 Block must contain exactly 1645 transactions of which the first has one input and script length 49", 49, theBitcoinBlock.getTransactions()[0].getListOfInputs()[0].getTxInScript().length);
		assertEquals("Random Version 3 Block must contain exactly 1645 transactions of which the first has two outputs", 2, theBitcoinBlock.getTransactions()[0].getListOfOutputs().length);
		assertEquals("Random Version 3 Block must contain exactly 1645 transactions of which the first has two output and the first output script length 25", 25, theBitcoinBlock.getTransactions()[0].getListOfOutputs()[0].getTxOutScript().length);
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseVersion4BlockAsBitcoinBlockHeap() throws FileNotFoundException, IOException, BitcoinBlockReadException {
            ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version4.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals("Random Version 4 Block must contain exactly 936 transactions", 936, theBitcoinBlock.getTransactions().length);
		assertEquals("Random Version 4 Block must contain exactly 936 transactions of which the first has one input", 1, theBitcoinBlock.getTransactions()[0].getListOfInputs().length);
		assertEquals("Random Version 4 Block must contain exactly 936 transactions of which the first has one input and script length 4", 4, theBitcoinBlock.getTransactions()[0].getListOfInputs()[0].getTxInScript().length);
		assertEquals("Random Version 4 Block must contain exactly 936 transactions of which the first has two outputs", 2, theBitcoinBlock.getTransactions()[0].getListOfOutputs().length);
		assertEquals("Random Version 4 Block must contain exactly 936 transactions of which the first has two output and the first output script length 25", 25, theBitcoinBlock.getTransactions()[0].getListOfOutputs()[0].getTxOutScript().length);
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }


  @Test
  public void parseGenesisBlockAsBitcoinBlockDirect() throws FileNotFoundException, IOException, BitcoinBlockReadException {
    ClassLoader classLoader = getClass().getClassLoader();
	String fileName="genesis.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals("Genesis Block must contain exactly one transaction", 1, theBitcoinBlock.getTransactions().length);
		assertEquals("Genesis Block must contain exactly one transaction with one input", 1, theBitcoinBlock.getTransactions()[0].getListOfInputs().length);
		assertEquals("Genesis Block must contain exactly one transaction with one input and script length 77", 77, theBitcoinBlock.getTransactions()[0].getListOfInputs()[0].getTxInScript().length);
		assertEquals("Genesis Block must contain exactly one transaction with one output", 1, theBitcoinBlock.getTransactions()[0].getListOfOutputs().length);
		assertEquals("Genesis Block must contain exactly one transaction with one output and script length 67", 67, theBitcoinBlock.getTransactions()[0].getListOfOutputs()[0].getTxOutScript().length);
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseVersion1BlockAsBitcoinBlockDirect() throws FileNotFoundException, IOException, BitcoinBlockReadException {
    ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version1.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals("Random Version 1 Block must contain exactly two transactions", 2, theBitcoinBlock.getTransactions().length);
		assertEquals("Random Version 1 Block must contain exactly two transactions of which the first has one input", 1, theBitcoinBlock.getTransactions()[0].getListOfInputs().length);
		assertEquals("Random Version 1 Block must contain exactly two transactions of which the first has one input and script length 8", 8, theBitcoinBlock.getTransactions()[0].getListOfInputs()[0].getTxInScript().length);
		assertEquals("Random Version 1 Block must contain exactly two transactions of which the first has one output", 1, theBitcoinBlock.getTransactions()[0].getListOfOutputs().length);
		assertEquals("Random Version 1 Block must contain exactly two transactions of which the first has one output and script length 67", 67, theBitcoinBlock.getTransactions()[0].getListOfOutputs()[0].getTxOutScript().length);
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseVersion2BlockAsBitcoinBlockDirect() throws FileNotFoundException, IOException, BitcoinBlockReadException {
      	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version2.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals("Random Version 2 Block must contain exactly 343 transactions", 343, theBitcoinBlock.getTransactions().length);
		assertEquals("Random Version 2 Block must contain exactly 343 transactions of which the first has one input", 1, theBitcoinBlock.getTransactions()[0].getListOfInputs().length);
		assertEquals("Random Version 2 Block must contain exactly 343 transactions of which the first has one input and script length 40", 40, theBitcoinBlock.getTransactions()[0].getListOfInputs()[0].getTxInScript().length);
		assertEquals("Random Version 2 Block must contain exactly 343 transactions of which the first has one output", 1, theBitcoinBlock.getTransactions()[0].getListOfOutputs().length);
		assertEquals("Random Version 2 Block must contain exactly 343 transactions of which the first has one output and script length 25", 25, theBitcoinBlock.getTransactions()[0].getListOfOutputs()[0].getTxOutScript().length);
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseVersion3BlockAsBitcoinBlockDirect() throws FileNotFoundException, IOException, BitcoinBlockReadException {
            ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version3.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals("Random Version 3 Block must contain exactly 1645 transactions", 1645, theBitcoinBlock.getTransactions().length);
		assertEquals("Random Version 3 Block must contain exactly 1645 transactions of which the first has one input", 1, theBitcoinBlock.getTransactions()[0].getListOfInputs().length);
		assertEquals("Random Version 3 Block must contain exactly 1645 transactions of which the first has one input and script length 49", 49, theBitcoinBlock.getTransactions()[0].getListOfInputs()[0].getTxInScript().length);
		assertEquals("Random Version 3 Block must contain exactly 1645 transactions of which the first has two outputs", 2, theBitcoinBlock.getTransactions()[0].getListOfOutputs().length);
		assertEquals("Random Version 3 Block must contain exactly 1645 transactions of which the first has two output and the first output script length 25", 25, theBitcoinBlock.getTransactions()[0].getListOfOutputs()[0].getTxOutScript().length);
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseVersion4BlockAsBitcoinBlockDirect() throws FileNotFoundException, IOException, BitcoinBlockReadException {
        ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version4.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals("Random Version 4 Block must contain exactly 936 transactions", 936, theBitcoinBlock.getTransactions().length);
		assertEquals("Random Version 4 Block must contain exactly 936 transactions of which the first has one input", 1, theBitcoinBlock.getTransactions()[0].getListOfInputs().length);
		assertEquals("Random Version 4 Block must contain exactly 936 transactions of which the first has one input and script length 4", 4, theBitcoinBlock.getTransactions()[0].getListOfInputs()[0].getTxInScript().length);
		assertEquals("Random Version 4 Block must contain exactly 936 transactions of which the first has two outputs", 2, theBitcoinBlock.getTransactions()[0].getListOfOutputs().length);
		assertEquals("Random Version 4 Block must contain exactly 936 transactions of which the first has two output and the first output script length 25", 25, theBitcoinBlock.getTransactions()[0].getListOfOutputs()[0].getTxOutScript().length);
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }



  @Test
  public void seekBlockStartHeap()  throws FileNotFoundException, IOException, BitcoinBlockReadException {
     ClassLoader classLoader = getClass().getClassLoader();
	String fileName="reqseekversion1.blk";
	String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameBlock);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		bbr.seekBlockStart();
		ByteBuffer version1ByteBuffer = bbr.readRawBlock();
		assertFalse("Random Version 1 Raw Block (requiring seek) is HeapByteBuffer", version1ByteBuffer.isDirect());
		assertEquals("Random Version 1 Raw Block (requiring seek) has a size of 482 bytes", 482, version1ByteBuffer.limit());
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

 @Test
  public void seekBlockStartDirect() throws FileNotFoundException, IOException, BitcoinBlockReadException  {
         ClassLoader classLoader = getClass().getClassLoader();
	String fileName="reqseekversion1.blk";
	String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameBlock);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		bbr.seekBlockStart();
		ByteBuffer version1ByteBuffer = bbr.readRawBlock();
		assertTrue("Random Version 1 Raw Block (requiring seek) is DirectByteBuffer", version1ByteBuffer.isDirect());
		assertEquals("Random Version 1 Raw Block (requiring seek) has a size of 482 bytes", 482, version1ByteBuffer.limit());
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

 @Test
  public void getKeyFromRawBlockHeap() throws FileNotFoundException, IOException, BitcoinBlockReadException {
ClassLoader classLoader = getClass().getClassLoader();
	String fileName="genesis.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameGenesis);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		ByteBuffer genesisByteBuffer = bbr.readRawBlock();
		assertFalse("Raw Genesis Block is HeapByteBuffer", genesisByteBuffer.isDirect());
		byte[] key = bbr.getKeyFromRawBlock(genesisByteBuffer);
		assertEquals("Raw Genesis Block Key should have a size of 64 bytes", 64, key.length);
		byte[] comparatorKey = new byte[]{(byte)0x3B,(byte)0xA3,(byte)0xED,(byte)0xFD,(byte)0x7A,(byte)0x7B,(byte)0x12,(byte)0xB2,(byte)0x7A,(byte)0xC7,(byte)0x2C,(byte)0x3E,(byte)0x67,(byte)0x76,(byte)0x8F,(byte)0x61,(byte)0x7F,(byte)0xC8,(byte)0x1B,(byte)0xC3,(byte)0x88,(byte)0x8A,(byte)0x51,(byte)0x32,(byte)0x3A,(byte)0x9F,(byte)0xB8,(byte)0xAA,(byte)0x4B,(byte)0x1E,(byte)0x5E,(byte)0x4A,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00};
		assertArrayEquals("Raw Genesis Block Key is equivalent to comparator key", comparatorKey, key);
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

 @Test
  public void getKeyFromRawBlockDirect() throws FileNotFoundException, IOException, BitcoinBlockReadException {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="genesis.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameGenesis);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		ByteBuffer genesisByteBuffer = bbr.readRawBlock();
		assertTrue("Raw Genesis Block is DirectByteBuffer", genesisByteBuffer.isDirect());
		byte[] key = bbr.getKeyFromRawBlock(genesisByteBuffer);
		assertEquals("Raw Genesis Block Key should have a size of 64 bytes", 64, key.length);
		byte[] comparatorKey = new byte[]{(byte)0x3B,(byte)0xA3,(byte)0xED,(byte)0xFD,(byte)0x7A,(byte)0x7B,(byte)0x12,(byte)0xB2,(byte)0x7A,(byte)0xC7,(byte)0x2C,(byte)0x3E,(byte)0x67,(byte)0x76,(byte)0x8F,(byte)0x61,(byte)0x7F,(byte)0xC8,(byte)0x1B,(byte)0xC3,(byte)0x88,(byte)0x8A,(byte)0x51,(byte)0x32,(byte)0x3A,(byte)0x9F,(byte)0xB8,(byte)0xAA,(byte)0x4B,(byte)0x1E,(byte)0x5E,(byte)0x4A,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00};
		assertArrayEquals("Raw Genesis Block Key is equivalent to comparator key", comparatorKey, key);
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
	}



} 


