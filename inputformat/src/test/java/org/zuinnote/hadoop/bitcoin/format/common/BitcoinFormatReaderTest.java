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


import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlockReader;
import org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException;


public class BitcoinFormatReaderTest {
static final int DEFAULT_BUFFERSIZE=64*1024;
static final int DEFAULT_MAXSIZE_BITCOINBLOCK=8 * 1024 * 1024;
static final byte[][] DEFAULT_MAGIC = {{(byte)0xF9,(byte)0xBE,(byte)0xB4,(byte)0xD9}};
private static final byte[][] TESTNET3_MAGIC = {{(byte)0x0B,(byte)0x11,(byte)0x09,(byte)0x07}};
private static final byte[][] MULTINET_MAGIC = {{(byte)0xF9,(byte)0xBE,(byte)0xB4,(byte)0xD9},{(byte)0x0B,(byte)0x11,(byte)0x09,(byte)0x07}};

 @Test
  public void checkTestDataGenesisBlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="genesis.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
	File file = new File(fileNameGenesis);
	assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
	assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
  }


 @Test
  public void checkTestDataVersion1BlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version1.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
	File file = new File(fileNameGenesis);
	assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
	assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
  }

 @Test
  public void checkTestDataVersion2BlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version2.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
	File file = new File(fileNameGenesis);
	assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
	assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
  }


 @Test
  public void checkTestDataVersion3BlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version3.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
	File file = new File(fileNameGenesis);
	assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
	assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
  }

 @Test
  public void checkTestDataVersion4BlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version4.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
	File file = new File(fileNameGenesis);
	assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
	assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
  }

 @Test
  public void checkTestDataVersion1SeekBlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="reqseekversion1.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
	File file = new File(fileNameGenesis);
	assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
	assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
  }

 @Test
  public void checkTestDataTestnet3GenesisBlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="testnet3genesis.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
	File file = new File(fileNameGenesis);
	assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
	assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
  }


 @Test
  public void checkTestDataTestnet3Version4BlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="testnet3version4.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
	File file = new File(fileNameGenesis);
	assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
	assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
  }



 @Test
  public void checkTestDataMultiNetAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="multinet.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
	File file = new File(fileNameGenesis);
	assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
	assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
  }
 
 @Test
 public void checkTestDataScriptWitnessNetAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="scriptwitness.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
	File file = new File(fileNameGenesis);
	assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
	assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
 }
 
 @Test
 public void checkTestDataScriptWitness2NetAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="scriptwitness2.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
	File file = new File(fileNameGenesis);
	assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
	assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
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
		assertFalse( genesisByteBuffer.isDirect(),"Raw Genesis Block is HeapByteBuffer");
		assertEquals( 293, genesisByteBuffer.limit(),"Raw Genesis block has a size of 293 bytes");
		
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
		assertFalse( version1ByteBuffer.isDirect(),"Random Version 1 Raw Block is HeapByteBuffer");
		assertEquals( 482, version1ByteBuffer.limit(),"Random Version 1 Raw Block has a size of 482 bytes");
		
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
		assertFalse( version2ByteBuffer.isDirect(),"Random Version 2 Raw Block is HeapByteBuffer");
		assertEquals( 191198, version2ByteBuffer.limit(),"Random Version 2 Raw Block has a size of 191.198 bytes");
		
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
		assertFalse( version3ByteBuffer.isDirect(),"Random Version 3 Raw Block is HeapByteBuffer");
		assertEquals( 932199, version3ByteBuffer.limit(),"Random Version 3 Raw Block has a size of 932.199 bytes");
		
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
		assertFalse( version4ByteBuffer.isDirect(),"Random Version 4 Raw Block is HeapByteBuffer");
		assertEquals( 998039, version4ByteBuffer.limit(),"Random Version 4 Raw Block has a size of 998.039 bytes");
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

@Test
  public void parseTestNet3GenesisBlockAsBitcoinRawBlockHeap() throws FileNotFoundException, IOException, BitcoinBlockReadException {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="testnet3genesis.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameGenesis);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.TESTNET3_MAGIC,direct);
		ByteBuffer genesisByteBuffer = bbr.readRawBlock();
		assertFalse( genesisByteBuffer.isDirect(),"Raw TestNet3 Genesis Block is HeapByteBuffer");
		assertEquals( 293, genesisByteBuffer.limit(),"Raw TestNet3 Genesis block has a size of 293 bytes");
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseTestNet3Version4BlockAsBitcoinRawBlockHeap()  throws FileNotFoundException, IOException, BitcoinBlockReadException {
    ClassLoader classLoader = getClass().getClassLoader();
	String fileName="testnet3version4.blk";
	String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameBlock);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.TESTNET3_MAGIC,direct);
		ByteBuffer version4ByteBuffer = bbr.readRawBlock();
		assertFalse( version4ByteBuffer.isDirect(),"Random TestNet3 Version 4 Raw Block is HeapByteBuffer");
		assertEquals( 749041, version4ByteBuffer.limit(),"Random TestNet3 Version 4 Raw Block has a size of 749.041 bytes");
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }


@Test
  public void parseMultiNetAsBitcoinRawBlockHeap() throws FileNotFoundException, IOException, BitcoinBlockReadException {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="multinet.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameGenesis);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.MULTINET_MAGIC,direct);
		ByteBuffer firstMultinetByteBuffer = bbr.readRawBlock();
		assertFalse( firstMultinetByteBuffer.isDirect(),"First MultiNetBlock is HeapByteBuffer");
		assertEquals( 293, firstMultinetByteBuffer.limit(),"First MultiNetBlock has a size of 293 bytes");
		ByteBuffer secondMultinetByteBuffer = bbr.readRawBlock();
		assertFalse( secondMultinetByteBuffer.isDirect(),"Second MultiNetBlock is HeapByteBuffer");
		assertEquals( 191198, secondMultinetByteBuffer.limit(),"Second MultiNetBlock has a size of 191.198 bytes");
		ByteBuffer thirdMultinetByteBuffer = bbr.readRawBlock();
		assertFalse( thirdMultinetByteBuffer.isDirect(),"Third MultiNetBlock is HeapByteBuffer");
		assertEquals( 749041, thirdMultinetByteBuffer.limit(),"Third MultiNetBlock has a size of 749.041 bytes");

		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

@Test
public void parseScriptWitnessBlockAsBitcoinRawBlockHeap()  throws FileNotFoundException, IOException, BitcoinBlockReadException {
  ClassLoader classLoader = getClass().getClassLoader();
	String fileName="scriptwitness.blk";
	String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameBlock);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		ByteBuffer scriptwitnessByteBuffer = bbr.readRawBlock();
		assertFalse( scriptwitnessByteBuffer.isDirect(),"Random ScriptWitness Raw Block is HeapByteBuffer");
		assertEquals( 999283, scriptwitnessByteBuffer.limit(),"Random ScriptWitness Raw Block has a size of 999283 bytes");
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
}


@Test
public void parseScriptWitness2BlockAsBitcoinRawBlockHeap()  throws FileNotFoundException, IOException, BitcoinBlockReadException {
  ClassLoader classLoader = getClass().getClassLoader();
	String fileName="scriptwitness2.blk";
	String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameBlock);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		ByteBuffer scriptwitnessByteBuffer = bbr.readRawBlock();
		assertFalse( scriptwitnessByteBuffer.isDirect(),"Random ScriptWitness Raw Block is HeapByteBuffer");
		assertEquals( 1000039, scriptwitnessByteBuffer.limit(),"Random ScriptWitness Raw Block has a size of 1000039 bytes");		
		scriptwitnessByteBuffer = bbr.readRawBlock();
		assertFalse( scriptwitnessByteBuffer.isDirect(),"Random ScriptWitness Raw Block is HeapByteBuffer");
		assertEquals( 999312, scriptwitnessByteBuffer.limit(),"Random ScriptWitness Raw Block has a size of 999312 bytes");		
		
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
		assertTrue( genesisByteBuffer.isDirect(),"Raw Genesis Block is DirectByteBuffer");
		assertEquals( 293, genesisByteBuffer.limit(),"Raw Genesis Block has a size of 293 bytes");
		
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
		assertTrue( version1ByteBuffer.isDirect(),"Random Version 1 Raw Block is DirectByteBuffer");
		assertEquals( 482, version1ByteBuffer.limit(),"Random Version 1 Raw Block has a size of 482 bytes");
		
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
		assertTrue( version2ByteBuffer.isDirect(),"Random Version 2 Raw Block is DirectByteBuffer");
		assertEquals( 191198, version2ByteBuffer.limit(),"Random Version 2 Raw Block has a size of 191.198 bytes");
		
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
		assertTrue( version3ByteBuffer.isDirect(),"Random Version 3 Raw Block is DirectByteBuffer");
		assertEquals( 932199, version3ByteBuffer.limit(),"Random Version 3 Raw Block has a size of 932.199 bytes");
		
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
		assertTrue( version4ByteBuffer.isDirect(),"Random Version 4 Raw Block is DirectByteBuffer");
		assertEquals( 998039, version4ByteBuffer.limit(),"Random Version 4 Raw Block has a size of 998.039 bytes");
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

@Test
  public void parseTestNet3GenesisBlockAsBitcoinRawBlockDirect() throws FileNotFoundException, IOException, BitcoinBlockReadException {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="testnet3genesis.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameGenesis);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.TESTNET3_MAGIC,direct);
		ByteBuffer genesisByteBuffer = bbr.readRawBlock();
		assertTrue( genesisByteBuffer.isDirect(),"Raw TestNet3 Genesis Block is DirectByteBuffer");
		assertEquals( 293, genesisByteBuffer.limit(),"Raw TestNet3 Genesis block has a size of 293 bytes");
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseTestNet3Version4BlockAsBitcoinRawBlockDirect()  throws FileNotFoundException, IOException, BitcoinBlockReadException {
    ClassLoader classLoader = getClass().getClassLoader();
	String fileName="testnet3version4.blk";
	String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameBlock);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.TESTNET3_MAGIC,direct);
		ByteBuffer version4ByteBuffer = bbr.readRawBlock();
		assertTrue( version4ByteBuffer.isDirect(),"Random TestNet3 Version 4 Raw Block is DirectByteBuffer");
		assertEquals( 749041, version4ByteBuffer.limit(),"Random TestNet3  Version 4 Raw Block has a size of 749.041 bytes");
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

@Test
  public void parseMultiNetAsBitcoinRawBlockDirect() throws FileNotFoundException, IOException, BitcoinBlockReadException {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="multinet.blk";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameGenesis);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.MULTINET_MAGIC,direct);
		ByteBuffer firstMultinetByteBuffer = bbr.readRawBlock();
		assertTrue( firstMultinetByteBuffer.isDirect(),"First MultiNetBlock is DirectByteBuffer");
		assertEquals( 293, firstMultinetByteBuffer.limit(),"First MultiNetBlock has a size of 293 bytes");
		ByteBuffer secondMultinetByteBuffer = bbr.readRawBlock();
		assertTrue( secondMultinetByteBuffer.isDirect(),"Second MultiNetBlock is DirectByteBuffer");
		assertEquals( 191198, secondMultinetByteBuffer.limit(),"Second MultiNetBlock has a size of 191.198 bytes");
		ByteBuffer thirdMultinetByteBuffer = bbr.readRawBlock();
		assertTrue( thirdMultinetByteBuffer.isDirect(),"Third MultiNetBlock is DirectByteBuffer");
		assertEquals( 749041, thirdMultinetByteBuffer.limit(),"Third MultiNetBlock has a size of 749.041 bytes");

		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

@Test
public void parseScriptWitnessBlockAsBitcoinRawBlockDirect()  throws FileNotFoundException, IOException, BitcoinBlockReadException {
  ClassLoader classLoader = getClass().getClassLoader();
	String fileName="scriptwitness.blk";
	String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameBlock);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		ByteBuffer scriptwitnessByteBuffer = bbr.readRawBlock();
		assertTrue( scriptwitnessByteBuffer.isDirect(),"Random ScriptWitness Raw Block is DirectByteBuffer");
		assertEquals( 999283, scriptwitnessByteBuffer.limit(),"Random ScriptWitness Raw Block has a size of 999283 bytes");
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
}

@Test
public void parseScriptWitness2BlockAsBitcoinRawBlockDirect()  throws FileNotFoundException, IOException, BitcoinBlockReadException {
  ClassLoader classLoader = getClass().getClassLoader();
	String fileName="scriptwitness2.blk";
	String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fileNameBlock);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		ByteBuffer scriptwitnessByteBuffer = bbr.readRawBlock();
		assertTrue( scriptwitnessByteBuffer.isDirect(),"Random ScriptWitness Raw Block is DirectByteBuffer");
		assertEquals( 1000039, scriptwitnessByteBuffer.limit(),"Random ScriptWitness Raw Block has a size of 1000039 bytes");		
		scriptwitnessByteBuffer = bbr.readRawBlock();
		assertTrue( scriptwitnessByteBuffer.isDirect(),"Random ScriptWitness Raw Block is DirectByteBuffer");
		assertEquals( 999312, scriptwitnessByteBuffer.limit(),"Random ScriptWitness Raw Block has a size of 999312 bytes");		
		
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
		assertEquals( 1, theBitcoinBlock.getTransactions().size(),"Genesis Block must contain exactly one transaction");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"Genesis Block must contain exactly one transaction with one input");
		assertEquals( 77, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Genesis Block must contain exactly one transaction with one input and script length 77");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"Genesis Block must contain exactly one transaction with one output");
		assertEquals( BigInteger.valueOf(5000000000L),theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getValue(), "Value must be BigInteger corresponding to 5000000000L");
		assertEquals( 67, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Genesis Block must contain exactly one transaction with one output and script length 67");
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }


  @Test
  public void parseTestNet3GenesisBlockAsBitcoinBlockHeap() throws FileNotFoundException, IOException, BitcoinBlockReadException {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="testnet3genesis.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.TESTNET3_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals( 1, theBitcoinBlock.getTransactions().size(),"TestNet3 Genesis Block must contain exactly one transaction");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"TestNet3 Genesis Block must contain exactly one transaction with one input");
		assertEquals( 77, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"TestNet3 Genesis Block must contain exactly one transaction with one input and script length 77");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"TestNet3 Genesis Block must contain exactly one transaction with one output");
		assertEquals( 67, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"TestNet3 Genesis Block must contain exactly one transaction with one output and script length 67");
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
		assertEquals( 2, theBitcoinBlock.getTransactions().size(),"Random Version 1 Block must contain exactly two transactions");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"Random Version 1 Block must contain exactly two transactions of which the first has one input");
		assertEquals( 8, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Random Version 1 Block must contain exactly two transactions of which the first has one input and script length 8");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"Random Version 1 Block must contain exactly two transactions of which the first has one output");
		assertEquals( 67, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Random Version 1 Block must contain exactly two transactions of which the first has one output and script length 67");
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
		assertEquals( 343, theBitcoinBlock.getTransactions().size(),"Random Version 2 Block must contain exactly 343 transactions");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"Random Version 2 Block must contain exactly 343 transactions of which the first has one input");
		assertEquals( 40, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Random Version 2 Block must contain exactly 343 transactions of which the first has one input and script length 40");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"Random Version 2 Block must contain exactly 343 transactions of which the first has one output");
		assertEquals( 25, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Random Version 2 Block must contain exactly 343 transactions of which the first has one output and script length 25");
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
		assertEquals( 1645, theBitcoinBlock.getTransactions().size(),"Random Version 3 Block must contain exactly 1645 transactions");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"Random Version 3 Block must contain exactly 1645 transactions of which the first has one input");
		assertEquals( 49, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Random Version 3 Block must contain exactly 1645 transactions of which the first has one input and script length 49");
		assertEquals( 2, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"Random Version 3 Block must contain exactly 1645 transactions of which the first has two outputs");
		assertEquals( 25, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Random Version 3 Block must contain exactly 1645 transactions of which the first has two output and the first output script length 25");
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
		assertEquals( 936, theBitcoinBlock.getTransactions().size(),"Random Version 4 Block must contain exactly 936 transactions");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"Random Version 4 Block must contain exactly 936 transactions of which the first has one input");
		assertEquals( 4, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Random Version 4 Block must contain exactly 936 transactions of which the first has one input and script length 4");
		assertEquals( 2, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"Random Version 4 Block must contain exactly 936 transactions of which the first has two outputs");
		assertEquals( 25, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Random Version 4 Block must contain exactly 936 transactions of which the first has two output and the first output script length 25");
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

  @Test
  public void parseTestNet3Version4BlockAsBitcoinBlockHeap() throws FileNotFoundException, IOException, BitcoinBlockReadException {
            ClassLoader classLoader = getClass().getClassLoader();
	String fileName="testnet3version4.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.TESTNET3_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals( 3299, theBitcoinBlock.getTransactions().size(),"Random TestNet3 Version 4 Block must contain exactly 3299 transactions");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"Random TestNet3 Version 4 Block must contain exactly 3299 transactions of which the first has one input");
		assertEquals( 35, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Random TestNet3 Version 4 Block must contain exactly 3299 transactions of which the first has one input and script length 35");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"Random TestNet3 Version 4 Block must contain exactly 3299 transactions of which the first has one outputs");
		assertEquals( 25, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Random TestNet3 Version 4 Block must contain exactly 3299 transactions of which the first has one output and the first output script length 25");
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

@Test
  public void parseMultiNetBlockAsBitcoinBlockHeap() throws FileNotFoundException, IOException, BitcoinBlockReadException {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="multinet.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.MULTINET_MAGIC,direct);
		BitcoinBlock firstBitcoinBlock = bbr.readBlock();
		assertEquals( 1, firstBitcoinBlock.getTransactions().size(),"First MultiNet Block must contain exactly one transaction");
		assertEquals( 1, firstBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"First MultiNet Block must contain exactly one transaction with one input");
		assertEquals( 77, firstBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"First MultiNet Block must contain exactly one transaction with one input and script length 77");
		assertEquals( 1, firstBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"First MultiNet Block must contain exactly one transaction with one output");
		assertEquals( 67, firstBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"First MultiNet Block must contain exactly one transaction with one output and script length 67");
		BitcoinBlock secondBitcoinBlock = bbr.readBlock();
		assertEquals( 343, secondBitcoinBlock.getTransactions().size(),"Second MultiNet Block must contain exactly 343 transactions");
		assertEquals( 1, secondBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"Second MultiNet Block must contain exactly 343 transactions of which the first has one input");
		assertEquals( 40, secondBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Second MultiNet Block must contain exactly 343 transactions of which the first has one input and script length 40");
		assertEquals( 1, secondBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"Second MultiNet Block must contain exactly 343 transactions of which the first has one output");
		assertEquals( 25, secondBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Second MultiNet Block must contain exactly 343 transactions of which the first has one output and script length 25");
		BitcoinBlock thirdBitcoinBlock = bbr.readBlock();
		assertEquals( 3299, thirdBitcoinBlock.getTransactions().size(),"Third MultiNet Block must contain exactly 3299 transactions");
		assertEquals( 1, thirdBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"Third MultiNet Block must contain exactly 3299 transactions of which the first has one input");
		assertEquals( 35, thirdBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Third MultiNet Block must contain exactly 3299 transactions of which the first has one input and script length 35");
		assertEquals( 1, thirdBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"Third MultiNet Block must contain exactly 3299 transactions of which the first has one outputs");
		assertEquals( 25, thirdBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Third MultiNet Block must contain exactly 3299 transactions of which the first has one output and the first output script length 25");
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

@Test
public void parseScriptWitnessBlockAsBitcoinBlockHeap() throws FileNotFoundException, IOException, BitcoinBlockReadException {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="scriptwitness.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals( 470, theBitcoinBlock.getTransactions().size(),"Random ScriptWitness Block must contain exactly 470 transactions");
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
}


@Test
public void parseScriptWitness2BlockAsBitcoinBlockHeap() throws FileNotFoundException, IOException, BitcoinBlockReadException {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="scriptwitness2.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals( 2191, theBitcoinBlock.getTransactions().size(),"First random ScriptWitness Block must contain exactly 2191 transactions");
		theBitcoinBlock = bbr.readBlock();
		assertEquals( 2508, theBitcoinBlock.getTransactions().size(),"Second random ScriptWitness Block must contain exactly 2508 transactions");
		
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
		assertEquals( 1, theBitcoinBlock.getTransactions().size(),"Genesis Block must contain exactly one transaction");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"Genesis Block must contain exactly one transaction with one input");
		assertEquals( 77, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Genesis Block must contain exactly one transaction with one input and script length 77");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"Genesis Block must contain exactly one transaction with one output");
		assertEquals( 67, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Genesis Block must contain exactly one transaction with one output and script length 67");
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }


  @Test
  public void parseTestNet3GenesisBlockAsBitcoinBlockDirect() throws FileNotFoundException, IOException, BitcoinBlockReadException {
    ClassLoader classLoader = getClass().getClassLoader();
	String fileName="testnet3genesis.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.TESTNET3_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals( 1, theBitcoinBlock.getTransactions().size(),"TestNet3 Genesis Block must contain exactly one transaction");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"TestNet3 Genesis Block must contain exactly one transaction with one input");
		assertEquals( 77, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"TestNet3 Genesis Block must contain exactly one transaction with one input and script length 77");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"TestNet3 Genesis Block must contain exactly one transaction with one output");
		assertEquals( 67, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"TestNet3 Genesis Block must contain exactly one transaction with one output and script length 67");
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
		assertEquals( 2, theBitcoinBlock.getTransactions().size(),"Random Version 1 Block must contain exactly two transactions");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"Random Version 1 Block must contain exactly two transactions of which the first has one input");
		assertEquals( 8, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Random Version 1 Block must contain exactly two transactions of which the first has one input and script length 8");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"Random Version 1 Block must contain exactly two transactions of which the first has one output");
		assertEquals( 67, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Random Version 1 Block must contain exactly two transactions of which the first has one output and script length 67");
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
		assertEquals( 343, theBitcoinBlock.getTransactions().size(),"Random Version 2 Block must contain exactly 343 transactions");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"Random Version 2 Block must contain exactly 343 transactions of which the first has one input");
		assertEquals( 40, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Random Version 2 Block must contain exactly 343 transactions of which the first has one input and script length 40");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"Random Version 2 Block must contain exactly 343 transactions of which the first has one output");
		assertEquals( 25, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Random Version 2 Block must contain exactly 343 transactions of which the first has one output and script length 25");
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
		assertEquals( 1645, theBitcoinBlock.getTransactions().size(),"Random Version 3 Block must contain exactly 1645 transactions");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"Random Version 3 Block must contain exactly 1645 transactions of which the first has one input");
		assertEquals( 49, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Random Version 3 Block must contain exactly 1645 transactions of which the first has one input and script length 49");
		assertEquals( 2, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"Random Version 3 Block must contain exactly 1645 transactions of which the first has two outputs");
		assertEquals( 25, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Random Version 3 Block must contain exactly 1645 transactions of which the first has two output and the first output script length 25");
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
		assertEquals( 936, theBitcoinBlock.getTransactions().size(),"Random Version 4 Block must contain exactly 936 transactions");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"Random Version 4 Block must contain exactly 936 transactions of which the first has one input");
		assertEquals( 4, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Random Version 4 Block must contain exactly 936 transactions of which the first has one input and script length 4");
		assertEquals( 2, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"Random Version 4 Block must contain exactly 936 transactions of which the first has two outputs");
		assertEquals( 25, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Random Version 4 Block must contain exactly 936 transactions of which the first has two output and the first output script length 25");
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }


  @Test
  public void parseTestNet3Version4BlockAsBitcoinBlockDirect() throws FileNotFoundException, IOException, BitcoinBlockReadException {
            ClassLoader classLoader = getClass().getClassLoader();
	String fileName="testnet3version4.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.TESTNET3_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals( 3299, theBitcoinBlock.getTransactions().size(),"Random TestNet3 Version 4 Block must contain exactly 3299 transactions");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"Random TestNet3 Version 4 Block must contain exactly 3299 transactions of which the first has one input");
		assertEquals( 35, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Random TestNet3 Version 4 Block must contain exactly 3299 transactions of which the first has one input and script length 35");
		assertEquals( 1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"Random TestNet3 Version 4 Block must contain exactly 3299 transactions of which the first has one outputs");
		assertEquals( 25, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Random TestNet3 Version 4 Block must contain exactly 3299 transactions of which the first has one output and the first output script length 25");
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }

@Test
  public void parseMultiNetBlockAsBitcoinBlockDirect() throws FileNotFoundException, IOException, BitcoinBlockReadException {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="multinet.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.MULTINET_MAGIC,direct);
		BitcoinBlock firstBitcoinBlock = bbr.readBlock();
		assertEquals( 1, firstBitcoinBlock.getTransactions().size(),"First MultiNet Block must contain exactly one transaction");
		assertEquals( 1, firstBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"First MultiNet Block must contain exactly one transaction with one input");
		assertEquals( 77, firstBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"First MultiNet Block must contain exactly one transaction with one input and script length 77");
		assertEquals( 1, firstBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"First MultiNet Block must contain exactly one transaction with one output");
		assertEquals( 67, firstBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"First MultiNet Block must contain exactly one transaction with one output and script length 67");
		BitcoinBlock secondBitcoinBlock = bbr.readBlock();
		assertEquals( 343, secondBitcoinBlock.getTransactions().size(),"Second MultiNet Block must contain exactly 343 transactions");
		assertEquals( 1, secondBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"Second MultiNet Block must contain exactly 343 transactions of which the first has one input");
		assertEquals( 40, secondBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Second MultiNet Block must contain exactly 343 transactions of which the first has one input and script length 40");
		assertEquals( 1, secondBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"Second MultiNet Block must contain exactly 343 transactions of which the first has one output");
		assertEquals( 25, secondBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Second MultiNet Block must contain exactly 343 transactions of which the first has one output and script length 25");
		BitcoinBlock thirdBitcoinBlock = bbr.readBlock();
		assertEquals( 3299, thirdBitcoinBlock.getTransactions().size(),"Third MultiNet Block must contain exactly 3299 transactions");
		assertEquals( 1, thirdBitcoinBlock.getTransactions().get(0).getListOfInputs().size(),"Third MultiNet Block must contain exactly 3299 transactions of which the first has one input");
		assertEquals( 35, thirdBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Third MultiNet Block must contain exactly 3299 transactions of which the first has one input and script length 35");
		assertEquals( 1, thirdBitcoinBlock.getTransactions().get(0).getListOfOutputs().size(),"Third MultiNet Block must contain exactly 3299 transactions of which the first has one outputs");
		assertEquals( 25, thirdBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Third MultiNet Block must contain exactly 3299 transactions of which the first has one output and the first output script length 25");
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }


@Test
public void parseScriptWitnessBlockAsBitcoinBlockDirect() throws FileNotFoundException, IOException, BitcoinBlockReadException {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="scriptwitness.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals( 470, theBitcoinBlock.getTransactions().size(),"Random ScriptWitness Block must contain exactly 470 transactions");
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
}

@Test
public void parseScriptWitness2BlockAsBitcoinBlockDirect() throws FileNotFoundException, IOException, BitcoinBlockReadException {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="scriptwitness2.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=true;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
		assertEquals( 2191, theBitcoinBlock.getTransactions().size(),"First random ScriptWitness Block must contain exactly 2191 transactions");
		theBitcoinBlock = bbr.readBlock();
		assertEquals( 2508, theBitcoinBlock.getTransactions().size(),"Second random ScriptWitness Block must contain exactly 2508 transactions");
		
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
		assertFalse( version1ByteBuffer.isDirect(),"Random Version 1 Raw Block (requiring seek) is HeapByteBuffer");
		assertEquals( 482, version1ByteBuffer.limit(),"Random Version 1 Raw Block (requiring seek) has a size of 482 bytes");
		
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
		assertTrue( version1ByteBuffer.isDirect(),"Random Version 1 Raw Block (requiring seek) is DirectByteBuffer");
		assertEquals( 482, version1ByteBuffer.limit(),"Random Version 1 Raw Block (requiring seek) has a size of 482 bytes");
		
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
		assertFalse( genesisByteBuffer.isDirect(),"Raw Genesis Block is HeapByteBuffer");
		byte[] key = bbr.getKeyFromRawBlock(genesisByteBuffer);
		assertEquals( 64, key.length,"Raw Genesis Block Key should have a size of 64 bytes");
		byte[] comparatorKey = new byte[]{(byte)0x3B,(byte)0xA3,(byte)0xED,(byte)0xFD,(byte)0x7A,(byte)0x7B,(byte)0x12,(byte)0xB2,(byte)0x7A,(byte)0xC7,(byte)0x2C,(byte)0x3E,(byte)0x67,(byte)0x76,(byte)0x8F,(byte)0x61,(byte)0x7F,(byte)0xC8,(byte)0x1B,(byte)0xC3,(byte)0x88,(byte)0x8A,(byte)0x51,(byte)0x32,(byte)0x3A,(byte)0x9F,(byte)0xB8,(byte)0xAA,(byte)0x4B,(byte)0x1E,(byte)0x5E,(byte)0x4A,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00};
		assertArrayEquals( comparatorKey, key,"Raw Genesis Block Key is equivalent to comparator key");
		
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
		assertTrue( genesisByteBuffer.isDirect(),"Raw Genesis Block is DirectByteBuffer");
		byte[] key = bbr.getKeyFromRawBlock(genesisByteBuffer);
		assertEquals( 64, key.length,"Raw Genesis Block Key should have a size of 64 bytes");
		byte[] comparatorKey = new byte[]{(byte)0x3B,(byte)0xA3,(byte)0xED,(byte)0xFD,(byte)0x7A,(byte)0x7B,(byte)0x12,(byte)0xB2,(byte)0x7A,(byte)0xC7,(byte)0x2C,(byte)0x3E,(byte)0x67,(byte)0x76,(byte)0x8F,(byte)0x61,(byte)0x7F,(byte)0xC8,(byte)0x1B,(byte)0xC3,(byte)0x88,(byte)0x8A,(byte)0x51,(byte)0x32,(byte)0x3A,(byte)0x9F,(byte)0xB8,(byte)0xAA,(byte)0x4B,(byte)0x1E,(byte)0x5E,(byte)0x4A,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00};
		assertArrayEquals( comparatorKey, key,"Raw Genesis Block Key is equivalent to comparator key");
		
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
	}



} 


