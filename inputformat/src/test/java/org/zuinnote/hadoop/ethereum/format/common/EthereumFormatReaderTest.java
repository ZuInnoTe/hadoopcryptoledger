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

package org.zuinnote.hadoop.ethereum.format.common;


import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.ethereum.format.exception.EthereumBlockReadException;

/**
 * @author jornfranke
 *
 */
public class EthereumFormatReaderTest {
	static final int DEFAULT_BUFFERSIZE=64*1024;
	static final int DEFAULT_MAXSIZE_ETHEREUMBLOCK=1 * 1024 * 1024;
	private static final Log LOG = LogFactory.getLog(EthereumFormatReaderTest.class.getName());
	public static int CHAIN_ID=1;
	
	 @Test
	  public void checkTestDataGenesisBlockAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="ethgenesis.bin";
		String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
		assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
		File file = new File(fileNameGenesis);
		assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
		assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
	  }
	 
	 @Test
	  public void checkTestDataBlock1Available() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth1.bin";
		String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
		assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
		File file = new File(fileNameGenesis);
		assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
		assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
	  }
	 
	 @Test
	  public void checkTestDataBlock1346406Available() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth1346406.bin";
		String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
		assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
		File file = new File(fileNameGenesis);
		assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
		assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
	  }
	 
	 @Test
	  public void checkTestDataBlock3346406Available() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth3346406.bin";
		String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
		assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
		File file = new File(fileNameGenesis);
		assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
		assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
	  }
	 
	 @Test
	  public void checkTestDataBlock0to10Available() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth0to10.bin";
		String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
		assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
		File file = new File(fileNameGenesis);
		assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
		assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
	  }
	 
	 @Test
	  public void checkTestDataBlock351000to3510010Available() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth351000to3510010.bin";
		String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
		assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
		File file = new File(fileNameGenesis);
		assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
		assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
	  }
	 
	 
	 @Test
	  public void checkTestDataBlock4800251Available() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth4800251.bin";
		String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
		assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
		File file = new File(fileNameGenesis);
		assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
		assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
	  }
	 
	 @Test
	  public void checkTestDataBlocks70000007000010Available() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth70000007000010";
		String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
		assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
		File file = new File(fileNameGenesis);
		assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
		assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
	  }
	 
	 @Test
	  public void parseGenesisBlockAsEthereumRawBlockHeap() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="ethgenesis.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			ByteBuffer blockByteBuffer = ebr.readRawBlock();
			assertFalse( blockByteBuffer.isDirect(),"Raw Genesis Block is HeapByteBuffer");
			assertEquals( 540, blockByteBuffer.limit(),"Raw Genesis block has a size of 540 bytes");
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseGenesisBlockAsEthereumRawBlockDirect() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="ethgenesis.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=true;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			ByteBuffer blockByteBuffer = ebr.readRawBlock();
			assertTrue( blockByteBuffer.isDirect(),"Raw Genesis Block is DirectByteBuffer");
			assertEquals( 540, blockByteBuffer.limit(),"Raw Genesis block has a size of 540 bytes");
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseBlockOneAsEthereumRawBlockHeap() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth1.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			ByteBuffer blockByteBuffer = ebr.readRawBlock();
			assertFalse( blockByteBuffer.isDirect(),"Raw block 1 is HeapByteBuffer");
			assertEquals( 537, blockByteBuffer.limit(),"Raw block 1 has a size of 537 bytes");
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseBlockOneAsEthereumRawBlockDirect() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth1.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=true;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			ByteBuffer blockByteBuffer = ebr.readRawBlock();
			assertTrue( blockByteBuffer.isDirect(),"Raw block 1 is DirectByteBuffer");
			assertEquals( 537, blockByteBuffer.limit(),"Raw block 1 has a size of 537 bytes");
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseBlock1346406AsEthereumRawBlockHeap() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth1346406.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			ByteBuffer blockByteBuffer = ebr.readRawBlock();
			assertFalse( blockByteBuffer.isDirect(),"Raw block 1346406 is HeapByteBuffer");
			assertEquals( 1223, blockByteBuffer.limit(),"Raw block 1346406 has a size of 1223 bytes");
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseBlock1346406AsEthereumRawBlockDirect() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth1346406.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=true;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			ByteBuffer blockByteBuffer = ebr.readRawBlock();
			assertTrue( blockByteBuffer.isDirect(),"Raw block 1346406 is DirectByteBuffer");
			assertEquals( 1223, blockByteBuffer.limit(),"Raw block 1346406 has a size of 1223 bytes");
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseBlock3346406AsEthereumRawBlockHeap() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth3346406.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			ByteBuffer blockByteBuffer = ebr.readRawBlock();
			assertFalse( blockByteBuffer.isDirect(),"Raw block 3346406 is HeapByteBuffer");
			assertEquals( 2251, blockByteBuffer.limit(),"Raw block 3346406 has a size of 2251 bytes");
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseBlock3346406AsEthereumRawBlockDirect() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth3346406.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=true;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			ByteBuffer blockByteBuffer = ebr.readRawBlock();
			assertTrue( blockByteBuffer.isDirect(),"Raw block 3346406 is DirectByteBuffer");
			assertEquals( 2251, blockByteBuffer.limit(),"Raw block 3346406 has a size of 2251 bytes");
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseBlock0to10AsEthereumRawBlockHeap() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth0to10.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			ByteBuffer blockByteBuffer = ebr.readRawBlock();
			assertFalse( blockByteBuffer.isDirect(),"Raw block 0 is HeapByteBuffer");
			assertEquals( 540, blockByteBuffer.limit(),"Raw block 0 has a size of 540 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 537, blockByteBuffer.limit(),"Raw block 1 has a size of 537 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 544, blockByteBuffer.limit(),"Raw block 2 has a size of 544 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 1079, blockByteBuffer.limit(),"Raw block 3 has a size of 1079 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 1079, blockByteBuffer.limit(),"Raw block 4 has a size of 1079 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 537, blockByteBuffer.limit(),"Raw block 5 has a size of 537 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 537, blockByteBuffer.limit(),"Raw block 6 has a size of 537 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 1078, blockByteBuffer.limit(),"Raw block 7 has a size of 1078 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 537, blockByteBuffer.limit(),"Raw block 8 has a size of 537 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 544, blockByteBuffer.limit(),"Raw block 9 has a size of 544 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 537, blockByteBuffer.limit(),"Raw block 10 has a size of 537 bytes");
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseBlock0to10AsEthereumRawBlockDirect() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth0to10.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=true;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			ByteBuffer blockByteBuffer = ebr.readRawBlock();
			assertTrue( blockByteBuffer.isDirect(),"Raw block 0 is DirectByteBuffer");
			assertEquals( 540, blockByteBuffer.limit(),"Raw block 0 has a size of 540 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 537, blockByteBuffer.limit(),"Raw block 1 has a size of 537 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 544, blockByteBuffer.limit(),"Raw block 2 has a size of 544 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 1079, blockByteBuffer.limit(),"Raw block 3 has a size of 1079 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 1079, blockByteBuffer.limit(),"Raw block 4 has a size of 1079 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 537, blockByteBuffer.limit(),"Raw block 5 has a size of 537 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 537, blockByteBuffer.limit(),"Raw block 6 has a size of 537 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 1078, blockByteBuffer.limit(),"Raw block 7 has a size of 1078 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 537, blockByteBuffer.limit(),"Raw block 8 has a size of 537 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 544, blockByteBuffer.limit(),"Raw block 9 has a size of 544 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 537, blockByteBuffer.limit(),"Raw block 10 has a size of 537 bytes");
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseBlock35100to351010AsEthereumRawBlockHeap() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth351000to3510010.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			ByteBuffer blockByteBuffer = ebr.readRawBlock();
			assertFalse( blockByteBuffer.isDirect(),"Raw block 3510000 is HeapByteBuffer");
			assertEquals( 2842, blockByteBuffer.limit(),"Raw block 3510000 has a size of 2842 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 539, blockByteBuffer.limit(),"Raw block 3510001 has a size of 539 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 2595, blockByteBuffer.limit(),"Raw block 3510002 has a size of 2595 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 11636, blockByteBuffer.limit(),"Raw block 3510003 has a size of 11636 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 1335, blockByteBuffer.limit(),"Raw block 3510004 has a size of 1335 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 9126, blockByteBuffer.limit(),"Raw block 3510005 has a size of 9126 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 7807, blockByteBuffer.limit(),"Raw block 3510006 has a size of 7807 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 532, blockByteBuffer.limit(),"Raw block 3510007 has a size of 532 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 1393, blockByteBuffer.limit(),"Raw block 3510008 has a size of 1393 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 1217, blockByteBuffer.limit(),"Raw block 3510009 has a size of 1217 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 1016, blockByteBuffer.limit(),"Raw block 3510010 has a size of 1016 bytes");
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 
	 @Test
	  public void parseBlock35100to351010AsEthereumRawBlockDirect() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth351000to3510010.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=true;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			ByteBuffer blockByteBuffer = ebr.readRawBlock();
			assertTrue( blockByteBuffer.isDirect(),"Raw block 3510000 is DirecteBuffer");
			assertEquals( 2842, blockByteBuffer.limit(),"Raw block 3510000 has a size of 2842 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 539, blockByteBuffer.limit(),"Raw block 3510001 has a size of 539 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 2595, blockByteBuffer.limit(),"Raw block 3510002 has a size of 2595 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 11636, blockByteBuffer.limit(),"Raw block 3510003 has a size of 11636 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 1335, blockByteBuffer.limit(),"Raw block 3510004 has a size of 1335 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 9126, blockByteBuffer.limit(),"Raw block 3510005 has a size of 9126 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 7807, blockByteBuffer.limit(),"Raw block 3510006 has a size of 7807 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 532, blockByteBuffer.limit(),"Raw block 3510007 has a size of 532 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 1393, blockByteBuffer.limit(),"Raw block 3510008 has a size of 1393 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 1217, blockByteBuffer.limit(),"Raw block 3510009 has a size of 1217 bytes");
			blockByteBuffer = ebr.readRawBlock();
			assertEquals( 1016, blockByteBuffer.limit(),"Raw block 3510010 has a size of 1016 bytes");
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseGenesisBlockAsEthereumBlockHeap() throws IOException, EthereumBlockReadException, ParseException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="ethgenesis.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			EthereumBlock eblock = ebr.readBlock();
			EthereumBlockHeader eblockHeader = eblock.getEthereumBlockHeader();
			List<EthereumTransaction> eTransactions = eblock.getEthereumTransactions();
			List<EthereumBlockHeader> eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Genesis block contains 0 transactions");
			assertEquals(0, eUncles.size(),"Genesis block contains 0 uncleHeaders");
			byte[] expectedParentHash = new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Genesis block contains a 32 byte hash consisting only of 0x00");
			byte[] expectedUncleHash = new byte[] {(byte) 0x1D,(byte) 0xCC,0x4D,(byte) 0xE8,(byte) 0xDE, (byte) 0xC7,(byte) 0x5D,
					(byte) 0x7A,(byte) 0xAB,(byte) 0x85,(byte) 0xB5,(byte) 0x67,(byte) 0xB6,(byte) 0xCC,(byte) 0xD4,
					0x1A,(byte) 0xD3,(byte)0x12, 0x45,0x1B,(byte) 0x94,(byte) 0x8A,0x74,0x13,(byte) 0xF0,
					(byte) 0xA1,0x42,(byte) 0xFD,0x40,(byte) 0xD4,(byte) 0x93,0x47};
			assertArrayEquals( expectedUncleHash, eblockHeader.getUncleHash(),"Genesis block contains a correct 32 byte uncle hash");
			byte[] expectedCoinbase = new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
			assertArrayEquals(expectedCoinbase,eblockHeader.getCoinBase(),"Genesis block contains a 20 byte coinbase consisting only of 0x00");
			byte[] expectedStateRoot= new byte[] {(byte) 0xD7,(byte) 0xF8,(byte) 0x97,0x4F,(byte) 0xB5,(byte) 0xAC,0x78,(byte) 0xD9,(byte) 0xAC,0x09,(byte) 0x9B,(byte) 0x9A,(byte) 0xD5,0x01,(byte) 0x8B,(byte) 0xED,(byte) 0xC2,(byte) 0xCE,0x0A,0x72,(byte) 0xDA,(byte) 0xD1,(byte) 0x82,0x7A,0x17,0x09,(byte) 0xDA,0x30,0x58,0x0F,0x05,0x44};
			assertArrayEquals(expectedStateRoot,eblockHeader.getStateRoot(),"Genesis block contains a correct 32 byte stateroot");
			byte[] expectedTxTrieRoot= new byte[] {0x56,(byte) 0xE8,0x1F,0x17,0x1B,(byte) 0xCC,0x55,(byte) 0xA6,(byte) 0xFF,(byte) 0x83,0x45,(byte) 0xE6,(byte) 0x92,(byte) 0xC0,(byte) 0xF8,0x6E,0x5B,0x48,(byte) 0xE0,0x1B,(byte) 0x99,0x6C,(byte) 0xAD,(byte) 0xC0,0x01,0x62,0x2F,(byte) 0xB5,(byte) 0xE3,0x63,(byte) 0xB4,0x21};
			assertArrayEquals(expectedTxTrieRoot,eblockHeader.getTxTrieRoot(),"Genesis block contains a correct 32 byte txTrieRoot");	
			byte[] expectedReceiptTrieRoot=new byte[] {0x56,(byte) 0xE8,0x1F,0x17,0x1B,(byte) 0xCC,0x55,(byte) 0xA6,(byte) 0xFF,(byte) 0x83,0x45,(byte) 0xE6,(byte) 0x92,(byte) 0xC0,(byte) 0xF8,0x6E,0x5B,0x48,(byte) 0xE0,0x1B,(byte) 0x99,0x6C,(byte) 0xAD,(byte) 0xC0,0x01,0x62,0x2F,(byte) 0xB5,(byte) 0xE3,0x63,(byte) 0xB4,0x21};
			assertArrayEquals(expectedReceiptTrieRoot,eblockHeader.getReceiptTrieRoot(),"Genesis block contains a correct 32 byte ReceiptTrieRoot");
			byte[] expectedLogsBloom = new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
			assertArrayEquals( expectedLogsBloom, eblockHeader.getLogsBloom(),"Genesis block contains a 256 byte log bloom consisting only of 0x00");
			byte[] expectedDifficulty = new byte[] {0x04,0x00,0x00,0x00,0x00};
			assertArrayEquals( expectedDifficulty, eblockHeader.getDifficulty(),"Genesis block contains a correct 5 byte difficulty");
			assertEquals(0L, eblockHeader.getTimestamp(),"Genesis block contains a timestamp of 0");
			long expectedNumber = 0L;
			assertEquals( BigInteger.valueOf(expectedNumber), eblockHeader.getNumber(),"Genesis block contains a number 0");
			byte[] expectedGasLimit = new byte[] {0x13,(byte) 0x88};
			assertArrayEquals( expectedGasLimit, eblockHeader.getGasLimitRaw(),"Genesis block contains a correct 2 byte gas limit");
			long expectedGasUsed = 0L;
			assertEquals( BigInteger.valueOf(expectedGasUsed), eblockHeader.getGasUsed(),"Genesis block contains a gas used of  0");
			byte[] expectedMixHash= new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
			assertArrayEquals( expectedMixHash, eblockHeader.getMixHash(),"Genesis block contains a correct 32 byte mix hash consisting only of 0x00");
			byte[] expectedExtraData= new byte[] {0x11,(byte) 0xBB,(byte) 0xE8,(byte) 0xDB,0x4E,0x34,0x7B,0x4E,(byte) 0x8C,(byte) 0x93,0x7C,0x1C,(byte) 0x83,0x70,(byte) 0xE4,(byte) 0xB5,(byte) 0xED,0x33,(byte) 0xAD,(byte) 0xB3,(byte) 0xDB,0x69,(byte) 0xCB,(byte) 0xDB,0x7A,0x38,(byte) 0xE1,(byte) 0xE5,0x0B,0x1B,(byte) 0x82,(byte) 0xFA};
			assertArrayEquals( expectedExtraData, eblockHeader.getExtraData(),"Genesis block contains correct 32 byte extra data");
			byte[] expectedNonce = new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x42};
			assertArrayEquals( expectedNonce, eblockHeader.getNonce(),"Genesis block contains a correct 8 byte nonce");
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 

	 
	 @Test
	  public void parseGenesisBlockAsEthereumBlockDirect() throws IOException, EthereumBlockReadException, ParseException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="ethgenesis.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=true;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			EthereumBlock eblock = ebr.readBlock();
			EthereumBlockHeader eblockHeader = eblock.getEthereumBlockHeader();
			List<EthereumTransaction> eTransactions = eblock.getEthereumTransactions();
			List<EthereumBlockHeader> eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Genesis block contains 0 transactions");
			assertEquals(0, eUncles.size(),"Genesis block contains 0 uncleHeaders");
			byte[] expectedParentHash = new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Genesis block contains a 32 byte hash consisting only of 0x00");
			byte[] expectedUncleHash = new byte[] {(byte) 0x1D,(byte) 0xCC,0x4D,(byte) 0xE8,(byte) 0xDE, (byte) 0xC7,(byte) 0x5D,
					(byte) 0x7A,(byte) 0xAB,(byte) 0x85,(byte) 0xB5,(byte) 0x67,(byte) 0xB6,(byte) 0xCC,(byte) 0xD4,
					0x1A,(byte) 0xD3,(byte)0x12, 0x45,0x1B,(byte) 0x94,(byte) 0x8A,0x74,0x13,(byte) 0xF0,
					(byte) 0xA1,0x42,(byte) 0xFD,0x40,(byte) 0xD4,(byte) 0x93,0x47};
			assertArrayEquals( expectedUncleHash, eblockHeader.getUncleHash(),"Genesis block contains a correct 32 byte uncle hash");
			byte[] expectedCoinbase = new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
			assertArrayEquals(expectedCoinbase,eblockHeader.getCoinBase(),"Genesis block contains a 20 byte coinbase consisting only of 0x00");
			byte[] expectedStateRoot= new byte[] {(byte) 0xD7,(byte) 0xF8,(byte) 0x97,0x4F,(byte) 0xB5,(byte) 0xAC,0x78,(byte) 0xD9,(byte) 0xAC,0x09,(byte) 0x9B,(byte) 0x9A,(byte) 0xD5,0x01,(byte) 0x8B,(byte) 0xED,(byte) 0xC2,(byte) 0xCE,0x0A,0x72,(byte) 0xDA,(byte) 0xD1,(byte) 0x82,0x7A,0x17,0x09,(byte) 0xDA,0x30,0x58,0x0F,0x05,0x44};
			assertArrayEquals(expectedStateRoot,eblockHeader.getStateRoot(),"Genesis block contains a correct 32 byte stateroot");
			byte[] expectedTxTrieRoot= new byte[] {0x56,(byte) 0xE8,0x1F,0x17,0x1B,(byte) 0xCC,0x55,(byte) 0xA6,(byte) 0xFF,(byte) 0x83,0x45,(byte) 0xE6,(byte) 0x92,(byte) 0xC0,(byte) 0xF8,0x6E,0x5B,0x48,(byte) 0xE0,0x1B,(byte) 0x99,0x6C,(byte) 0xAD,(byte) 0xC0,0x01,0x62,0x2F,(byte) 0xB5,(byte) 0xE3,0x63,(byte) 0xB4,0x21};
			assertArrayEquals(expectedTxTrieRoot,eblockHeader.getTxTrieRoot(),"Genesis block contains a correct 32 byte txTrieRoot");	
			byte[] expectedReceiptTrieRoot=new byte[] {0x56,(byte) 0xE8,0x1F,0x17,0x1B,(byte) 0xCC,0x55,(byte) 0xA6,(byte) 0xFF,(byte) 0x83,0x45,(byte) 0xE6,(byte) 0x92,(byte) 0xC0,(byte) 0xF8,0x6E,0x5B,0x48,(byte) 0xE0,0x1B,(byte) 0x99,0x6C,(byte) 0xAD,(byte) 0xC0,0x01,0x62,0x2F,(byte) 0xB5,(byte) 0xE3,0x63,(byte) 0xB4,0x21};
			assertArrayEquals(expectedReceiptTrieRoot,eblockHeader.getReceiptTrieRoot(),"Genesis block contains a correct 32 byte ReceiptTrieRoot");
			byte[] expectedLogsBloom = new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
			assertArrayEquals( expectedLogsBloom, eblockHeader.getLogsBloom(),"Genesis block contains a 256 byte logs bloom consisting only of 0x00");
			byte[] expectedDifficulty = new byte[] {0x04,0x00,0x00,0x00,0x00};
			assertArrayEquals( expectedDifficulty, eblockHeader.getDifficulty(),"Genesis block contains a correct 5 byte difficulty");
			assertEquals(0L, eblockHeader.getTimestamp(),"Genesis block contains a timestamp of 0");
			long expectedNumber = 0L;
			assertEquals( BigInteger.valueOf(expectedNumber), eblockHeader.getNumber(),"Genesis block contains a number 0");
			byte[] expectedGasLimit = new byte[] {0x13,(byte) 0x88};
			assertArrayEquals( expectedGasLimit, eblockHeader.getGasLimitRaw(),"Genesis block contains a correct 2 byte gas limit");
			long expectedGasUsed = 0L;
			assertEquals( BigInteger.valueOf(expectedGasUsed), eblockHeader.getGasUsed(),"Genesis block contains a gas used of  0");
			byte[] expectedMixHash= new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
			assertArrayEquals( expectedMixHash, eblockHeader.getMixHash(),"Genesis block contains a correct 32 byte mix hash consisting only of 0x00");
			byte[] expectedExtraData= new byte[] {0x11,(byte) 0xBB,(byte) 0xE8,(byte) 0xDB,0x4E,0x34,0x7B,0x4E,(byte) 0x8C,(byte) 0x93,0x7C,0x1C,(byte) 0x83,0x70,(byte) 0xE4,(byte) 0xB5,(byte) 0xED,0x33,(byte) 0xAD,(byte) 0xB3,(byte) 0xDB,0x69,(byte) 0xCB,(byte) 0xDB,0x7A,0x38,(byte) 0xE1,(byte) 0xE5,0x0B,0x1B,(byte) 0x82,(byte) 0xFA};
			assertArrayEquals( expectedExtraData, eblockHeader.getExtraData(),"Genesis block contains correct 32 byte extra data");
			byte[] expectedNonce = new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x42};
			assertArrayEquals( expectedNonce, eblockHeader.getNonce(),"Genesis block contains a correct 8 byte nonce");
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseBlockOneAsEthereumBlockHeap() throws IOException, EthereumBlockReadException, ParseException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth1.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			EthereumBlock eblock = ebr.readBlock();
			EthereumBlockHeader eblockHeader = eblock.getEthereumBlockHeader();
			List<EthereumTransaction> eTransactions = eblock.getEthereumTransactions();
			List<EthereumBlockHeader> eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block contains 0 uncleHeaders");
			byte[] expectedParentHash = new byte[] {(byte) 0xD4,(byte) 0xE5,0x67,0x40,(byte) 0xF8,0x76,(byte) 0xAE,(byte) 0xF8,(byte) 0xC0,0x10,(byte) 0xB8,0x6A,0x40,(byte) 0xD5,(byte) 0xF5,0x67,0x45,(byte) 0xA1,0x18,(byte) 0xD0,(byte) 0x90,0x6A,0x34,(byte) 0xE6,(byte) 0x9A,(byte) 0xEC,(byte) 0x8C,0x0D,(byte) 0xB1,(byte) 0xCB,(byte) 0x8F,(byte) 0xA3};
			
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block contains a correct 32 byte parent hash");
			byte[] expectedUncleHash = new byte[] {(byte) 0x1D,(byte) 0xCC,0x4D,(byte) 0xE8,(byte) 0xDE, (byte) 0xC7,(byte) 0x5D,
					(byte) 0x7A,(byte) 0xAB,(byte) 0x85,(byte) 0xB5,(byte) 0x67,(byte) 0xB6,(byte) 0xCC,(byte) 0xD4,
					0x1A,(byte) 0xD3,(byte)0x12, 0x45,0x1B,(byte) 0x94,(byte) 0x8A,0x74,0x13,(byte) 0xF0,
					(byte) 0xA1,0x42,(byte) 0xFD,0x40,(byte) 0xD4,(byte) 0x93,0x47};
			assertArrayEquals( expectedUncleHash, eblockHeader.getUncleHash(),"Block contains a correct 32 byte uncle hash");
			byte[] expectedCoinbase = new byte[] {(byte)0x05,(byte)0xA5,(byte)0x6E,(byte)0x2D,(byte)0x52,(byte)0xC8,(byte)0x17,(byte)0x16,(byte)0x18,(byte)0x83,(byte)0xF5,(byte)0x0C,(byte)0x44,(byte)0x1C,(byte)0x32,(byte)0x28,(byte)0xCF,(byte)0xE5,(byte)0x4D,(byte)0x9F};
			assertArrayEquals(expectedCoinbase,eblockHeader.getCoinBase(),"Block contains a correct  20 byte coinbase");
			byte[] expectedStateRoot= new byte[] {(byte)0xD6,(byte)0x7E,(byte)0x4D,(byte)0x45,(byte)0x03,(byte)0x43,(byte)0x04,(byte)0x64,(byte)0x25,(byte)0xAE,(byte)0x42,(byte)0x71,(byte)0x47,(byte)0x43,(byte)0x53,(byte)0x85,(byte)0x7A,(byte)0xB8,(byte)0x60,(byte)0xDB,(byte)0xC0,(byte)0xA1,(byte)0xDD,(byte)0xE6,(byte)0x4B,(byte)0x41,(byte)0xB5,(byte)0xCD,(byte)0x3A,(byte)0x53,(byte)0x2B,(byte)0xF3};
			assertArrayEquals(expectedStateRoot,eblockHeader.getStateRoot(),"Block contains a correct 32 byte stateroot");
			byte[] expectedTxTrieRoot= new byte[] {0x56,(byte) 0xE8,0x1F,0x17,0x1B,(byte) 0xCC,0x55,(byte) 0xA6,(byte) 0xFF,(byte) 0x83,0x45,(byte) 0xE6,(byte) 0x92,(byte) 0xC0,(byte) 0xF8,0x6E,0x5B,0x48,(byte) 0xE0,0x1B,(byte) 0x99,0x6C,(byte) 0xAD,(byte) 0xC0,0x01,0x62,0x2F,(byte) 0xB5,(byte) 0xE3,0x63,(byte) 0xB4,0x21};
			assertArrayEquals(expectedTxTrieRoot,eblockHeader.getTxTrieRoot(),"Block contains a correct 32 byte txTrieRoot");	
			byte[] expectedReceiptTrieRoot=new byte[] {0x56,(byte) 0xE8,0x1F,0x17,0x1B,(byte) 0xCC,0x55,(byte) 0xA6,(byte) 0xFF,(byte) 0x83,0x45,(byte) 0xE6,(byte) 0x92,(byte) 0xC0,(byte) 0xF8,0x6E,0x5B,0x48,(byte) 0xE0,0x1B,(byte) 0x99,0x6C,(byte) 0xAD,(byte) 0xC0,0x01,0x62,0x2F,(byte) 0xB5,(byte) 0xE3,0x63,(byte) 0xB4,0x21};
			assertArrayEquals(expectedReceiptTrieRoot,eblockHeader.getReceiptTrieRoot(),"Block contains a correct 32 byte ReceiptTrieRoot");
			byte[] expectedLogsBloom = new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
			assertArrayEquals( expectedLogsBloom, eblockHeader.getLogsBloom(),"Block contains a 256 byte log bloom consisting only of 0x00");
			byte[] expectedDifficulty = new byte[] {0x03,(byte) 0xFF,(byte) 0x80,0x00,0x00};
			assertArrayEquals( expectedDifficulty, eblockHeader.getDifficulty(),"Block contains a correct 5 byte difficulty");
			DateFormat format = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z");
			String expectedDTStr = "30-07-2015 15:26:28 UTC";
			long expectedTimestamp = format.parse(expectedDTStr).getTime() / 1000;
			//1438269988
			assertEquals(expectedTimestamp, eblockHeader.getTimestamp(),"Block contains a timestamp of "+expectedDTStr);
			long expectedNumber = 1L;
			assertEquals( BigInteger.valueOf(expectedNumber), eblockHeader.getNumber(),"Block contains a number 1");
			byte[] expectedGasLimit = new byte[] {0x13,(byte) 0x88};
			assertArrayEquals( expectedGasLimit, eblockHeader.getGasLimitRaw(),"Block contains a correct 2 byte gas limit");
			long expectedGasUsed = 0L;
			assertEquals( BigInteger.valueOf(expectedGasUsed), eblockHeader.getGasUsed(),"Block contains a gas used of  0");
			byte[] expectedMixHash= new byte[] {(byte)0x96,(byte)0x9B,(byte)0x90,(byte)0x0D,(byte)0xE2,(byte)0x7B,(byte)0x6A,(byte)0xC6,(byte)0xA6,(byte)0x77,(byte)0x42,(byte)0x36,(byte)0x5D,(byte)0xD6,(byte)0x5F,(byte)0x55,(byte)0xA0,(byte)0x52,(byte)0x6C,(byte)0x41,(byte)0xFD,(byte)0x18,(byte)0xE1,(byte)0xB1,(byte)0x6F,(byte)0x1A,(byte)0x12,(byte)0x15,(byte)0xC2,(byte)0xE6,(byte)0x6F,(byte)0x59};
			assertArrayEquals( expectedMixHash, eblockHeader.getMixHash(),"Block contains a correct 32 byte mix hash");
			byte[] expectedExtraData= new byte[] {(byte)0x47,(byte)0x65,(byte)0x74,(byte)0x68,(byte)0x2F,(byte)0x76,(byte)0x31,(byte)0x2E,(byte)0x30,(byte)0x2E,(byte)0x30,(byte)0x2F,(byte)0x6C,(byte)0x69,(byte)0x6E,(byte)0x75,(byte)0x78,(byte)0x2F,(byte)0x67,(byte)0x6F,(byte)0x31,(byte)0x2E,(byte)0x34,(byte)0x2E,(byte)0x32};
			// corresponds to Geth/v1.0.0/linux/go1.4.2
			assertArrayEquals( expectedExtraData, eblockHeader.getExtraData(),"Block contains correct 32 byte extra data");
			byte[] expectedNonce = new byte[] {(byte)0x53,(byte)0x9B,(byte)0xD4,(byte)0x97,(byte)0x9F,(byte)0xEF,(byte)0x1E,(byte)0xC4};
			assertArrayEquals( expectedNonce, eblockHeader.getNonce(),"Block contains a correct 8 byte nonce");
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseBlockOneAsEthereumBlockDirect() throws IOException, EthereumBlockReadException, ParseException {
			ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth1.bin";
			String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
			File file = new File(fileNameBlock);
			boolean direct=true;
			FileInputStream fin = new FileInputStream(file);
			EthereumBlockReader ebr = null;
			try {
				ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
				EthereumBlock eblock = ebr.readBlock();
				EthereumBlockHeader eblockHeader = eblock.getEthereumBlockHeader();
				List<EthereumTransaction> eTransactions = eblock.getEthereumTransactions();
				List<EthereumBlockHeader> eUncles = eblock.getUncleHeaders();
				assertEquals( 0, eTransactions.size(),"Block contains 0 transactions");
				assertEquals(0, eUncles.size(),"Block contains 0 uncleHeaders");
				byte[] expectedParentHash = new byte[] {(byte) 0xD4,(byte) 0xE5,0x67,0x40,(byte) 0xF8,0x76,(byte) 0xAE,(byte) 0xF8,(byte) 0xC0,0x10,(byte) 0xB8,0x6A,0x40,(byte) 0xD5,(byte) 0xF5,0x67,0x45,(byte) 0xA1,0x18,(byte) 0xD0,(byte) 0x90,0x6A,0x34,(byte) 0xE6,(byte) 0x9A,(byte) 0xEC,(byte) 0x8C,0x0D,(byte) 0xB1,(byte) 0xCB,(byte) 0x8F,(byte) 0xA3};
				
				assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block contains a correct 32 byte parent hash");
				byte[] expectedUncleHash = new byte[] {(byte) 0x1D,(byte) 0xCC,0x4D,(byte) 0xE8,(byte) 0xDE, (byte) 0xC7,(byte) 0x5D,
						(byte) 0x7A,(byte) 0xAB,(byte) 0x85,(byte) 0xB5,(byte) 0x67,(byte) 0xB6,(byte) 0xCC,(byte) 0xD4,
						0x1A,(byte) 0xD3,(byte)0x12, 0x45,0x1B,(byte) 0x94,(byte) 0x8A,0x74,0x13,(byte) 0xF0,
						(byte) 0xA1,0x42,(byte) 0xFD,0x40,(byte) 0xD4,(byte) 0x93,0x47};
				assertArrayEquals( expectedUncleHash, eblockHeader.getUncleHash(),"Block contains a correct 32 byte uncle hash");
				byte[] expectedCoinbase = new byte[] {(byte)0x05,(byte)0xA5,(byte)0x6E,(byte)0x2D,(byte)0x52,(byte)0xC8,(byte)0x17,(byte)0x16,(byte)0x18,(byte)0x83,(byte)0xF5,(byte)0x0C,(byte)0x44,(byte)0x1C,(byte)0x32,(byte)0x28,(byte)0xCF,(byte)0xE5,(byte)0x4D,(byte)0x9F};
				assertArrayEquals(expectedCoinbase,eblockHeader.getCoinBase(),"Block contains a correct  20 byte coinbase");
				byte[] expectedStateRoot= new byte[] {(byte)0xD6,(byte)0x7E,(byte)0x4D,(byte)0x45,(byte)0x03,(byte)0x43,(byte)0x04,(byte)0x64,(byte)0x25,(byte)0xAE,(byte)0x42,(byte)0x71,(byte)0x47,(byte)0x43,(byte)0x53,(byte)0x85,(byte)0x7A,(byte)0xB8,(byte)0x60,(byte)0xDB,(byte)0xC0,(byte)0xA1,(byte)0xDD,(byte)0xE6,(byte)0x4B,(byte)0x41,(byte)0xB5,(byte)0xCD,(byte)0x3A,(byte)0x53,(byte)0x2B,(byte)0xF3};
				assertArrayEquals(expectedStateRoot,eblockHeader.getStateRoot(),"Block contains a correct 32 byte stateroot");
				byte[] expectedTxTrieRoot= new byte[] {0x56,(byte) 0xE8,0x1F,0x17,0x1B,(byte) 0xCC,0x55,(byte) 0xA6,(byte) 0xFF,(byte) 0x83,0x45,(byte) 0xE6,(byte) 0x92,(byte) 0xC0,(byte) 0xF8,0x6E,0x5B,0x48,(byte) 0xE0,0x1B,(byte) 0x99,0x6C,(byte) 0xAD,(byte) 0xC0,0x01,0x62,0x2F,(byte) 0xB5,(byte) 0xE3,0x63,(byte) 0xB4,0x21};
				assertArrayEquals(expectedTxTrieRoot,eblockHeader.getTxTrieRoot(),"Block contains a correct 32 byte txTrieRoot");	
				byte[] expectedReceiptTrieRoot=new byte[] {0x56,(byte) 0xE8,0x1F,0x17,0x1B,(byte) 0xCC,0x55,(byte) 0xA6,(byte) 0xFF,(byte) 0x83,0x45,(byte) 0xE6,(byte) 0x92,(byte) 0xC0,(byte) 0xF8,0x6E,0x5B,0x48,(byte) 0xE0,0x1B,(byte) 0x99,0x6C,(byte) 0xAD,(byte) 0xC0,0x01,0x62,0x2F,(byte) 0xB5,(byte) 0xE3,0x63,(byte) 0xB4,0x21};
				assertArrayEquals(expectedReceiptTrieRoot,eblockHeader.getReceiptTrieRoot(),"Block contains a correct 32 byte ReceiptTrieRoot");
				byte[] expectedLogsBloom = new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
				assertArrayEquals( expectedLogsBloom, eblockHeader.getLogsBloom(),"Block contains a 256 byte log bloom consisting only of 0x00");
				byte[] expectedDifficulty = new byte[] {0x03,(byte) 0xFF,(byte) 0x80,0x00,0x00};
				assertArrayEquals( expectedDifficulty, eblockHeader.getDifficulty(),"Block contains a correct 5 byte difficulty");
				DateFormat format = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z");
				String expectedDTStr = "30-07-2015 15:26:28 UTC";
				long expectedTimestamp = format.parse(expectedDTStr).getTime() / 1000;
				//1438269988
				assertEquals(expectedTimestamp, eblockHeader.getTimestamp(),"Block contains a timestamp of "+expectedDTStr);
				long expectedNumber = 1L;
				assertEquals( BigInteger.valueOf(expectedNumber), eblockHeader.getNumber(),"Block contains a number 1");
				byte[] expectedGasLimit = new byte[] {0x13,(byte) 0x88};
				assertArrayEquals( expectedGasLimit, eblockHeader.getGasLimitRaw(),"Block contains a correct 2 byte gas limit");
				long expectedGasUsed = 0L;
				assertEquals( BigInteger.valueOf(expectedGasUsed), eblockHeader.getGasUsed(),"Block contains a gas used of  0");
				byte[] expectedMixHash= new byte[] {(byte)0x96,(byte)0x9B,(byte)0x90,(byte)0x0D,(byte)0xE2,(byte)0x7B,(byte)0x6A,(byte)0xC6,(byte)0xA6,(byte)0x77,(byte)0x42,(byte)0x36,(byte)0x5D,(byte)0xD6,(byte)0x5F,(byte)0x55,(byte)0xA0,(byte)0x52,(byte)0x6C,(byte)0x41,(byte)0xFD,(byte)0x18,(byte)0xE1,(byte)0xB1,(byte)0x6F,(byte)0x1A,(byte)0x12,(byte)0x15,(byte)0xC2,(byte)0xE6,(byte)0x6F,(byte)0x59};
				assertArrayEquals( expectedMixHash, eblockHeader.getMixHash(),"Block contains a correct 32 byte mix hash");
				byte[] expectedExtraData= new byte[] {(byte)0x47,(byte)0x65,(byte)0x74,(byte)0x68,(byte)0x2F,(byte)0x76,(byte)0x31,(byte)0x2E,(byte)0x30,(byte)0x2E,(byte)0x30,(byte)0x2F,(byte)0x6C,(byte)0x69,(byte)0x6E,(byte)0x75,(byte)0x78,(byte)0x2F,(byte)0x67,(byte)0x6F,(byte)0x31,(byte)0x2E,(byte)0x34,(byte)0x2E,(byte)0x32};
				// corresponds to Geth/v1.0.0/linux/go1.4.2
				assertArrayEquals( expectedExtraData, eblockHeader.getExtraData(),"Block contains correct 32 byte extra data");
				byte[] expectedNonce = new byte[] {(byte)0x53,(byte)0x9B,(byte)0xD4,(byte)0x97,(byte)0x9F,(byte)0xEF,(byte)0x1E,(byte)0xC4};
				assertArrayEquals( expectedNonce, eblockHeader.getNonce(),"Block contains a correct 8 byte nonce");
			} finally {
				if (ebr!=null) {
					ebr.close();
				}
			}
	  }

	 
	 @Test
	  public void parseBlock1346406AsEthereumBlockHeap() throws IOException, EthereumBlockReadException, ParseException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth1346406.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			EthereumBlock eblock = ebr.readBlock();
			EthereumBlockHeader eblockHeader = eblock.getEthereumBlockHeader();
			List<EthereumTransaction> eTransactions = eblock.getEthereumTransactions();
			List<EthereumBlockHeader> eUncles = eblock.getUncleHeaders();
			assertEquals( 6, eTransactions.size(),"Block contains 6 transactions");
			assertEquals(0, eUncles.size(),"Block contains 0 uncleHeaders");
			byte[] expectedParentHash = new byte[] {(byte)0xBA,(byte)0x6D,(byte)0xD2,(byte)0x60,(byte)0x12,(byte)0xB3,(byte)0x71,(byte)0x90,(byte)0x48,(byte)0xF3,(byte)0x16,(byte)0xC6,(byte)0xED,(byte)0xB3,(byte)0x34,(byte)0x9B,(byte)0xDF,(byte)0xBD,(byte)0x61,(byte)0x31,(byte)0x9F,(byte)0xA9,(byte)0x7C,(byte)0x61,(byte)0x6A,(byte)0x61,(byte)0x31,(byte)0x18,(byte)0xA1,(byte)0xAF,(byte)0x30,(byte)0x67};
			
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block contains a correct 32 byte parent hash");
			byte[] expectedUncleHash = new byte[] {(byte) 0x1D,(byte) 0xCC,0x4D,(byte) 0xE8,(byte) 0xDE, (byte) 0xC7,(byte) 0x5D,
					(byte) 0x7A,(byte) 0xAB,(byte) 0x85,(byte) 0xB5,(byte) 0x67,(byte) 0xB6,(byte) 0xCC,(byte) 0xD4,
					0x1A,(byte) 0xD3,(byte)0x12, 0x45,0x1B,(byte) 0x94,(byte) 0x8A,0x74,0x13,(byte) 0xF0,
					(byte) 0xA1,0x42,(byte) 0xFD,0x40,(byte) 0xD4,(byte) 0x93,0x47};
			assertArrayEquals( expectedUncleHash, eblockHeader.getUncleHash(),"Block contains a correct 32 byte uncle hash");
			byte[] expectedCoinbase = new byte[] {(byte)0x1A,(byte)0x06,(byte)0x0B,(byte)0x06,(byte)0x04,(byte)0x88,(byte)0x3A,(byte)0x99,(byte)0x80,(byte)0x9E,(byte)0xB3,(byte)0xF7,(byte)0x98,(byte)0xDF,(byte)0x71,(byte)0xBE,(byte)0xF6,(byte)0xC3,(byte)0x58,(byte)0xF1};
			assertArrayEquals(expectedCoinbase,eblockHeader.getCoinBase(),"Block contains a correct  20 byte coinbase");
			byte[] expectedStateRoot= new byte[] {(byte)0x21,(byte)0xBA,(byte)0x88,(byte)0x6F,(byte)0xD2,(byte)0x6F,(byte)0x17,(byte)0xB4,(byte)0x01,(byte)0xF5,(byte)0x39,(byte)0x20,(byte)0x15,(byte)0x33,(byte)0x10,(byte)0xB6,(byte)0x93,(byte)0x9B,(byte)0xAD,(byte)0x8A,(byte)0x5F,(byte)0xC3,(byte)0xBF,(byte)0x8C,(byte)0x50,(byte)0x5C,(byte)0x55,(byte)0x6D,(byte)0xDB,(byte)0xAF,(byte)0xBC,(byte)0x5C};
			assertArrayEquals(expectedStateRoot,eblockHeader.getStateRoot(),"Block contains a correct 32 byte stateroot");
			byte[] expectedTxTrieRoot= new byte[] {(byte)0xB3,(byte)0xCB,(byte)0xC7,(byte)0xF0,(byte)0xD7,(byte)0x87,(byte)0xE5,(byte)0x7D,(byte)0x93,(byte)0x70,(byte)0xB8,(byte)0x02,(byte)0xAB,(byte)0x94,(byte)0x5E,(byte)0x21,(byte)0x99,(byte)0x1C,(byte)0x3E,(byte)0x12,(byte)0x7D,(byte)0x70,(byte)0x12,(byte)0x0C,(byte)0x37,(byte)0xE9,(byte)0xFD,(byte)0xAE,(byte)0x3E,(byte)0xF3,(byte)0xEB,(byte)0xFC};
			assertArrayEquals(expectedTxTrieRoot,eblockHeader.getTxTrieRoot(),"Block contains a correct 32 byte txTrieRoot");	
			byte[] expectedReceiptTrieRoot=new byte[] {(byte)0x9B,(byte)0xCE,(byte)0x71,(byte)0x32,(byte)0xF5,(byte)0x2D,(byte)0x4D,(byte)0x45,(byte)0xA8,(byte)0xA2,(byte)0x47,(byte)0x48,(byte)0x47,(byte)0x86,(byte)0xC7,(byte)0x0B,(byte)0xB2,(byte)0xE6,(byte)0x39,(byte)0x59,(byte)0xC8,(byte)0x56,(byte)0x1B,(byte)0x3A,(byte)0xBF,(byte)0xD4,(byte)0xE7,(byte)0x22,(byte)0xE6,(byte)0x00,(byte)0x6A,(byte)0x27};
			assertArrayEquals(expectedReceiptTrieRoot,eblockHeader.getReceiptTrieRoot(),"Block contains a correct 32 byte ReceiptTrieRoot");
			byte[] expectedLogsBloom = new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
			assertArrayEquals( expectedLogsBloom, eblockHeader.getLogsBloom(),"Block contains a 256 byte log bloom consisting only of 0x00");
			byte[] expectedDifficulty = new byte[] {0x19,(byte) 0xFF,(byte) 0x9E,(byte) 0xC4,0x35,(byte) 0xE0};
	
			assertArrayEquals( expectedDifficulty, eblockHeader.getDifficulty(),"Block contains a correct 5 byte difficulty");
			DateFormat format = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z");
			String expectedDTStr = "16-04-2016 09:34:29 UTC";
			long expectedTimestamp = format.parse(expectedDTStr).getTime() / 1000;
			//1438269988
			assertEquals(expectedTimestamp, eblockHeader.getTimestamp(),"Block contains a timestamp of "+expectedDTStr);
			long expectedNumber = 1346406L;
			assertEquals( BigInteger.valueOf(expectedNumber), eblockHeader.getNumber(),"Block contains a number 1346406");
			byte[] expectedGasLimit = new byte[] {0x47,(byte) 0xE7,(byte) 0xC4}; 
			assertArrayEquals( expectedGasLimit, eblockHeader.getGasLimitRaw(),"Block contains a correct 3 byte gas limit");
			long expectedGasUsed = 126000L;
			assertEquals( BigInteger.valueOf(expectedGasUsed), eblockHeader.getGasUsed(),"Block contains a gas used of  126000");
			byte[] expectedMixHash= new byte[] {(byte)0x4F,(byte)0x57,(byte)0x71,(byte)0xB7,(byte)0x9A,(byte)0x8E,(byte)0x6E,(byte)0x21,(byte)0x99,(byte)0x35,(byte)0x53,(byte)0x9C,(byte)0x47,(byte)0x3E,(byte)0x23,(byte)0xBA,(byte)0xFD,(byte)0x2C,(byte)0xA3,(byte)0x5C,(byte)0xC1,(byte)0x86,(byte)0x20,(byte)0x66,(byte)0x31,(byte)0xC3,(byte)0xB0,(byte)0x9E,(byte)0xD5,(byte)0x76,(byte)0x19,(byte)0x4A};
			assertArrayEquals( expectedMixHash, eblockHeader.getMixHash(),"Block contains a correct 32 byte mix hash");
			byte[] expectedExtraData= new byte[] {(byte)0xD7,(byte)0x83,(byte)0x01,(byte)0x03,(byte)0x05,(byte)0x84,(byte)0x47,(byte)0x65,(byte)0x74,(byte)0x68,(byte)0x87,(byte)0x67,(byte)0x6F,(byte)0x31,(byte)0x2E,(byte)0x35,(byte)0x2E,(byte)0x31,(byte)0x85,(byte)0x6C,(byte)0x69,(byte)0x6E,(byte)0x75,(byte)0x78};
			// corresponds to 010305/Geth/go1.5.1/linux
			assertArrayEquals( expectedExtraData, eblockHeader.getExtraData(),"Block contains correct 24 byte extra data");
			byte[] expectedNonce = new byte[] {(byte)0xFF,(byte)0x7C,(byte)0x7A,(byte)0xEE,(byte)0x0E,(byte)0x88,(byte)0xC5,(byte)0x2D};
			assertArrayEquals( expectedNonce, eblockHeader.getNonce(),"Block contains a correct 8 byte nonce");
			// check transactions
			// 1st transaction
			int transactNum=0;
			byte[] expected1stNonce = new byte[] {0x0c};
			assertArrayEquals(expected1stNonce, eblock.getEthereumTransactions().get(transactNum).getNonce(),"Transaction 1 has a correct nonce");
			byte[] expected1stGasPrice = new byte[] {0x04,(byte) 0xA8,0x17,(byte) 0xC8,0x00};
			assertArrayEquals(expected1stGasPrice, eblock.getEthereumTransactions().get(transactNum).getGasPriceRaw(),"Transaction 1 has a correct gas price");
			byte[] expected1stGasLimit = new byte[] {(byte) 0x52,0x08};
			assertArrayEquals(expected1stGasLimit, eblock.getEthereumTransactions().get(transactNum).getGasLimitRaw(),"Transaction 1 has a correct gas limit");
			byte[] expected1stReceiveAddress = new byte[] {(byte)0x1E,(byte)0x75,(byte)0xF0,(byte)0x2A,(byte)0x6E,(byte)0x9F,(byte)0xF4,(byte)0xFF,(byte)0x16,(byte)0x33,(byte)0x38,(byte)0x25,(byte)0xD9,(byte)0x09,(byte)0xBB,(byte)0x03,(byte)0x33,(byte)0x06,(byte)0xB7,(byte)0x8B};
			assertArrayEquals(expected1stReceiveAddress, eblock.getEthereumTransactions().get(transactNum).getReceiveAddress(),"Transaction 1 has a correct receive address");
			byte[] expected1stValue = new byte[] {0x0E,(byte) 0xD5,(byte) 0xDA,(byte) 0xBC,(byte) 0x91,0x7D,(byte) 0xAC,0x00};
			assertArrayEquals(expected1stValue, eblock.getEthereumTransactions().get(transactNum).getValueRaw(),"Transaction 1 has a correct value");	
			byte[] expected1stData = new byte[] {};
			assertArrayEquals(expected1stData, eblock.getEthereumTransactions().get(transactNum).getData(),"Transaction 1 has a correct data");
			byte[] expected1stsigv = new byte[] {0x1B};
			assertArrayEquals(expected1stsigv, eblock.getEthereumTransactions().get(transactNum).getSig_v(),"Transaction 1 has a correct sigv");
			byte[] expected1stsigr = new byte[] {(byte)0x47,(byte)0xDD,(byte)0xF9,(byte)0x37,(byte)0x68,(byte)0x97,(byte)0x76,(byte)0x78,(byte)0x13,(byte)0x95,(byte)0x5A,(byte)0x9D,(byte)0x46,(byte)0xB6,(byte)0xF1,(byte)0xAA,(byte)0x77,(byte)0x73,(byte)0xE5,(byte)0xC8,(byte)0xC6,(byte)0x21,(byte)0x67,(byte)0x54,(byte)0x1C,(byte)0x80,(byte)0xBF,(byte)0x25,(byte)0x2D,(byte)0xC7,(byte)0xDC,(byte)0xD2};
			assertArrayEquals(expected1stsigr, eblock.getEthereumTransactions().get(transactNum).getSig_r(),"Transaction 1 has a correct sigr");
			byte[] expected1stsigs = new byte[] {(byte)0x0A,(byte)0x31,(byte)0x3F,(byte)0x35,(byte)0x60,(byte)0x32,(byte)0x37,(byte)0x56,(byte)0xB7,(byte)0x28,(byte)0x5F,(byte)0x62,(byte)0x38,(byte)0x51,(byte)0x86,(byte)0x05,(byte)0x82,(byte)0x1A,(byte)0x2B,(byte)0xEE,(byte)0x03,(byte)0x7D,(byte)0xEA,(byte)0x8F,(byte)0x09,(byte)0x22,(byte)0x66,(byte)0x20,(byte)0x89,(byte)0x03,(byte)0x74,(byte)0x59};
			assertArrayEquals(expected1stsigs, eblock.getEthereumTransactions().get(transactNum).getSig_s(),"Transaction 1 has a correct sigs");
			// 2nd transaction
		    transactNum=1;
			byte[] expected2ndNonce = new byte[] {(byte) 0xff,(byte) 0xD7};
			assertArrayEquals(expected2ndNonce, eblock.getEthereumTransactions().get(transactNum).getNonce(),"Transaction 2 has a correct nonce");
			byte[] expected2ndGasPrice = new byte[] {0x04,(byte) 0xA8,0x17,(byte) 0xC8,0x00};
			assertArrayEquals(expected2ndGasPrice,eblock.getEthereumTransactions().get(transactNum).getGasPriceRaw(),"Transaction 2 has a correct gas price");
			byte[] expected2ndGasLimit = new byte[] {(byte) 0x01,0x5F,(byte) 0x90};
			assertArrayEquals(expected2ndGasLimit, eblock.getEthereumTransactions().get(transactNum).getGasLimitRaw(),"Transaction 2 has a correct gas limit");
			byte[] expected2ndReceiveAddress = new byte[] {(byte)0x54,(byte)0x67,(byte)0xFA,(byte)0xBD,(byte)0x30,(byte)0xEB,(byte)0x61,(byte)0xA1,(byte)0x84,(byte)0x61,(byte)0xD1,(byte)0x53,(byte)0xD8,(byte)0xC6,(byte)0xFF,(byte)0xB1,(byte)0x9D,(byte)0xD4,(byte)0x7A,(byte)0x25};
			assertArrayEquals(expected2ndReceiveAddress, eblock.getEthereumTransactions().get(transactNum).getReceiveAddress(),"Transaction 2 has a correct receive address");
			byte[] expected2ndValue = new byte[] {0x46,(byte) 0xEC,(byte) 0x2C,(byte) 0x96,(byte) 0x05,0x0B,(byte) 0x18,0x00};
	
			assertArrayEquals(expected2ndValue, eblock.getEthereumTransactions().get(transactNum).getValueRaw(),"Transaction 2 has a correct value");	
			byte[] expected2ndData = new byte[] {};
			assertArrayEquals(expected2ndData, eblock.getEthereumTransactions().get(transactNum).getData(),"Transaction 2 has a correct data");
			byte[] expected2ndsigv = new byte[] {0x1B};
			assertArrayEquals(expected2ndsigv, eblock.getEthereumTransactions().get(transactNum).getSig_v(),"Transaction 2 has a correct sigv");
			byte[] expected2ndsigr = new byte[] {(byte)0x62,(byte)0x85,(byte)0x3C,(byte)0x63,(byte)0x9A,(byte)0x9B,(byte)0x9E,(byte)0xC4,(byte)0x9B,(byte)0xA9,(byte)0xAC,(byte)0x53,(byte)0xE2,(byte)0x85,(byte)0xB3,(byte)0x4E,(byte)0xD0,(byte)0xB7,(byte)0x65,(byte)0x5C,(byte)0x1B,(byte)0xE3,(byte)0x29,(byte)0xFB,(byte)0x8B,(byte)0x34,(byte)0x70,(byte)0x74,(byte)0x0C,(byte)0x3D,(byte)0x0A,(byte)0x9A};
			assertArrayEquals(expected2ndsigr, eblock.getEthereumTransactions().get(transactNum).getSig_r(),"Transaction 2 has a correct sigr");
			byte[] expected2ndsigs = new byte[] {(byte)0x03,(byte)0xFA,(byte)0xA6,(byte)0xF4,(byte)0xFF,(byte)0x1A,(byte)0x45,(byte)0x76,(byte)0xDF,(byte)0x08,(byte)0x9A,(byte)0x9F,(byte)0x9C,(byte)0xB7,(byte)0x9C,(byte)0xF2,(byte)0xED,(byte)0xC1,(byte)0xC5,(byte)0xBD,(byte)0xEC,(byte)0x0F,(byte)0xE7,(byte)0x9C,(byte)0x79,(byte)0x2A,(byte)0xCB,(byte)0x9E,(byte)0x83,(byte)0xF2,(byte)0x41};
			assertArrayEquals(expected2ndsigs, eblock.getEthereumTransactions().get(transactNum).getSig_s(),"Transaction 2 has a correct sigs");

			// 3rd transaction
		    transactNum=2;
			byte[] expected3rdNonce = new byte[] {0x02,(byte) 0xD7,(byte) 0xDD};
			assertArrayEquals(expected3rdNonce, eblock.getEthereumTransactions().get(transactNum).getNonce(),"Transaction 3 has a correct nonce");
			byte[] expected3rdGasPrice = new byte[] {0x04,(byte) 0xA8,0x17,(byte) 0xC8,0x00};
			assertArrayEquals(expected3rdGasPrice, eblock.getEthereumTransactions().get(transactNum).getGasPriceRaw(),"Transaction 3 has a correct gas price");
			byte[] expected3rdGasLimit = new byte[] {(byte) 0x01,0x5F,(byte) 0x90};
			assertArrayEquals(expected3rdGasLimit,eblock.getEthereumTransactions().get(transactNum).getGasLimitRaw(),"Transaction 3 has a correct gas limit");
			byte[] expected3rdReceiveAddress = new byte[] {(byte)0xB4,(byte)0xD0,(byte)0xCA,(byte)0x2B,(byte)0x7E,(byte)0x4C,(byte)0xB1,(byte)0xE0,(byte)0x61,(byte)0x0D,(byte)0x02,(byte)0x15,(byte)0x4A,(byte)0x10,(byte)0x16,(byte)0x3A,(byte)0xB0,(byte)0xF4,(byte)0x2E,(byte)0x65};
			assertArrayEquals(expected3rdReceiveAddress, eblock.getEthereumTransactions().get(transactNum).getReceiveAddress(),"Transaction 3 has a correct receive address");
			byte[] expected3rdValue = new byte[] {(byte) 0x29,(byte) 0x73,(byte) 0xCD,(byte) 0x62,0x4F,(byte) 0x70,0x00};
			assertArrayEquals(expected3rdValue, eblock.getEthereumTransactions().get(transactNum).getValueRaw(),"Transaction 3 has a correct value");	
			byte[] expected3rdData = new byte[] {};
			assertArrayEquals(expected3rdData, eblock.getEthereumTransactions().get(transactNum).getData(),"Transaction 3 has a correct data");
			byte[] expected3rdsigv = new byte[] {0x1C};
			assertArrayEquals(expected3rdsigv, eblock.getEthereumTransactions().get(transactNum).getSig_v(),"Transaction 3 has a correct sigv");
			byte[] expected3rdsigr = new byte[]  {(byte)0x89,(byte)0xFD,(byte)0x7A,(byte)0x62,(byte)0xCF,(byte)0x44,(byte)0x77,(byte)0xBF,(byte)0xE5,(byte)0xDB,(byte)0xF0,(byte)0xEE,(byte)0xCF,(byte)0x3A,(byte)0x4A,(byte)0x96,(byte)0x71,(byte)0x96,(byte)0x96,(byte)0xFB,(byte)0xBE,(byte)0x16,(byte)0xBA,(byte)0x0A,(byte)0xBA,(byte)0x1D,(byte)0x63,(byte)0x1D,(byte)0x44,(byte)0xC1,(byte)0xEB,(byte)0x58};
			assertArrayEquals(expected3rdsigr, eblock.getEthereumTransactions().get(transactNum).getSig_r(),"Transaction 3 has a correct sigr");
			byte[] expected3rdsigs = new byte[] {(byte)0x24,(byte)0x34,(byte)0x48,(byte)0x64,(byte)0xEB,(byte)0x6A,(byte)0x60,(byte)0xC6,(byte)0x6F,(byte)0xB5,(byte)0xDA,(byte)0xED,(byte)0x02,(byte)0xB5,(byte)0x63,(byte)0x52,(byte)0xE8,(byte)0x17,(byte)0x42,(byte)0x16,(byte)0xB8,(byte)0xA2,(byte)0xD3,(byte)0x33,(byte)0xB7,(byte)0xF3,(byte)0x32,(byte)0xFF,(byte)0x6B,(byte)0xA0,(byte)0x69,(byte)0x9C};
			assertArrayEquals(expected3rdsigs, eblock.getEthereumTransactions().get(transactNum).getSig_s(),"Transaction 3 has a correct sigs");

			// 4th transaction
		    transactNum=3;
			byte[] expected4thNonce = new byte[] {0x02,(byte) 0xD7,(byte) 0xDE};
			assertArrayEquals(expected4thNonce, eblock.getEthereumTransactions().get(transactNum).getNonce(),"Transaction 4 has a correct nonce");
			byte[] expected4thGasPrice = new byte[] {0x04,(byte) 0xA8,0x17,(byte) 0xC8,0x00};
			assertArrayEquals(expected4thGasPrice, eblock.getEthereumTransactions().get(transactNum).getGasPriceRaw(),"Transaction 4 has a correct gas price");
			byte[] expected4thGasLimit = new byte[] {(byte) 0x01,0x5F,(byte) 0x90};
			assertArrayEquals(expected4thGasLimit, eblock.getEthereumTransactions().get(transactNum).getGasLimitRaw(),"Transaction 4 has a correct gas limit");
			byte[] expected4thReceiveAddress = new byte[] {(byte)0x1F,(byte)0x57,(byte)0xF8,(byte)0x26,(byte)0xCA,(byte)0xF5,(byte)0x94,(byte)0xF7,(byte)0xA8,(byte)0x37,(byte)0xD9,(byte)0xFC,(byte)0x09,(byte)0x24,(byte)0x56,(byte)0x87,(byte)0x0A,(byte)0x28,(byte)0x93,(byte)0x65};
			assertArrayEquals(expected4thReceiveAddress, eblock.getEthereumTransactions().get(transactNum).getReceiveAddress(),"Transaction 4 has a correct receive address");
			byte[] expected4thValue = new byte[] {0x01,(byte) 0xD1,(byte) 0x4C,(byte) 0xAC,(byte) 0xFB,0x05,(byte) 0xC4,0x00};
			assertArrayEquals(expected4thValue, eblock.getEthereumTransactions().get(transactNum).getValueRaw(),"Transaction 4 has a correct value");	
			byte[] expected4thData = new byte[] {};
			assertArrayEquals(expected4thData, eblock.getEthereumTransactions().get(transactNum).getData(),"Transaction 4 has a correct data");
			byte[] expected4thsigv = new byte[] {0x1B};
			assertArrayEquals(expected4thsigv, eblock.getEthereumTransactions().get(transactNum).getSig_v(),"Transaction 4 has a correct sigv");
			byte[] expected4thsigr = new byte[] {(byte)0x46,(byte)0x01,(byte)0x57,(byte)0xDC,(byte)0xE4,(byte)0xE9,(byte)0x5D,(byte)0x1D,(byte)0xCC,(byte)0x7A,(byte)0xED,(byte)0x0D,(byte)0x9B,(byte)0x7E,(byte)0x3D,(byte)0x65,(byte)0x37,(byte)0x0C,(byte)0x53,(byte)0xD2,(byte)0x9E,(byte)0xA9,(byte)0xB1,(byte)0xAA,(byte)0x4C,(byte)0x9C,(byte)0x22,(byte)0x14,(byte)0x91,(byte)0x1C,(byte)0xD9,(byte)0x5E};
			assertArrayEquals(expected4thsigr, eblock.getEthereumTransactions().get(transactNum).getSig_r(),"Transaction 4 has a correct sigr");
			byte[] expected4thsigs = new byte[] {(byte)0x6A,(byte)0x84,(byte)0x4F,(byte)0x95,(byte)0x6D,(byte)0x02,(byte)0x46,(byte)0x94,(byte)0x1B,(byte)0x94,(byte)0x30,(byte)0x91,(byte)0x34,(byte)0x21,(byte)0x20,(byte)0xBD,(byte)0x48,(byte)0xE7,(byte)0xC6,(byte)0x35,(byte)0x77,(byte)0xF0,(byte)0xBA,(byte)0x3D,(byte)0x87,(byte)0x59,(byte)0xC9,(byte)0xEC,(byte)0x58,(byte)0x70,(byte)0x4E,(byte)0xEC};
			assertArrayEquals(expected4thsigs, eblock.getEthereumTransactions().get(transactNum).getSig_s(),"Transaction 4 has a correct sigs");

			// 5th transaction
		    transactNum=4;
			byte[] expected5thNonce = new byte[] {0x02,(byte) 0xD7,(byte) 0xDF};
			assertArrayEquals(expected5thNonce, eblock.getEthereumTransactions().get(transactNum).getNonce(),"Transaction 5 has a correct nonce");
			byte[] expected5thGasPrice = new byte[] {0x04,(byte) 0xA8,0x17,(byte) 0xC8,0x00};
			assertArrayEquals(expected5thGasPrice, eblock.getEthereumTransactions().get(transactNum).getGasPriceRaw(),"Transaction 5 has a correct gas price");
			byte[] expected5thGasLimit = new byte[] {(byte) 0x01,0x5F,(byte) 0x90};
			assertArrayEquals(expected5thGasLimit, eblock.getEthereumTransactions().get(transactNum).getGasLimitRaw(),"Transaction 5 has a correct gas limit");
			byte[] expected5thReceiveAddress = new byte[] {(byte)0x1F,(byte)0x57,(byte)0xF8,(byte)0x26,(byte)0xCA,(byte)0xF5,(byte)0x94,(byte)0xF7,(byte)0xA8,(byte)0x37,(byte)0xD9,(byte)0xFC,(byte)0x09,(byte)0x24,(byte)0x56,(byte)0x87,(byte)0x0A,(byte)0x28,(byte)0x93,(byte)0x65};
			assertArrayEquals(expected5thReceiveAddress, eblock.getEthereumTransactions().get(transactNum).getReceiveAddress(),"Transaction 5 has a correct receive address");
			byte[] expected5thValue = new byte[] {0x02,(byte) 0x02,(byte) 0x05,(byte) 0x26,(byte) 0x47,(byte) 0xC0,(byte) 0xF0,0x00};
			assertArrayEquals(expected5thValue, eblock.getEthereumTransactions().get(transactNum).getValueRaw(),"Transaction 5 has a correct value");	
			byte[] expected5thData = new byte[] {};
			assertArrayEquals(expected5thData, eblock.getEthereumTransactions().get(transactNum).getData(),"Transaction 5 has a correct data");
			byte[] expected5thsigv = new byte[] {0x1C};
			assertArrayEquals(expected5thsigv, eblock.getEthereumTransactions().get(transactNum).getSig_v(),"Transaction 5 has a correct sigv");
			byte[] expected5thsigr = new byte[] {(byte)0xE4,(byte)0xBE,(byte)0x97,(byte)0xD5,(byte)0xAF,(byte)0xF1,(byte)0xB5,(byte)0xE7,(byte)0x99,(byte)0x12,(byte)0x96,(byte)0x98,(byte)0x2B,(byte)0xDF,(byte)0xC1,(byte)0xC2,(byte)0x2F,(byte)0x75,(byte)0x21,(byte)0x13,(byte)0x4F,(byte)0x7E,(byte)0x1A,(byte)0x9D,(byte)0xA3,(byte)0x00,(byte)0x42,(byte)0x0D,(byte)0xAD,(byte)0x33,(byte)0x6F,(byte)0x34};
			assertArrayEquals(expected5thsigr, eblock.getEthereumTransactions().get(transactNum).getSig_r(),"Transaction 5 has a correct sigr");
			byte[] expected5thsigs = new byte[] {(byte)0x62,(byte)0xDE,(byte)0xF8,(byte)0xAA,(byte)0x83,(byte)0x65,(byte)0x58,(byte)0xC7,(byte)0xB0,(byte)0xA5,(byte)0x65,(byte)0xB9,(byte)0x7C,(byte)0x9B,(byte)0x27,(byte)0xB2,(byte)0x0E,(byte)0xD9,(byte)0xA0,(byte)0x51,(byte)0xDE,(byte)0x22,(byte)0xAD,(byte)0x8D,(byte)0xBD,(byte)0x62,(byte)0x52,(byte)0x44,(byte)0xCE,(byte)0x64,(byte)0x9E,(byte)0x3D};
			assertArrayEquals(expected5thsigs, eblock.getEthereumTransactions().get(transactNum).getSig_s(),"Transaction 5 has a correct sigs");

			// 6th transaction
		    transactNum=5;
			byte[] expected6thNonce = new byte[] {0x02,(byte) 0xD7,(byte) 0xE0};
			assertArrayEquals(expected6thNonce, eblock.getEthereumTransactions().get(transactNum).getNonce(),"Transaction 6 has a correct nonce");
			byte[] expected6thGasPrice = new byte[] {0x04,(byte) 0xA8,0x17,(byte) 0xC8,0x00};
			assertArrayEquals(expected6thGasPrice, eblock.getEthereumTransactions().get(transactNum).getGasPriceRaw(),"Transaction 6 has a correct gas price");
			byte[] expected6thGasLimit = new byte[] {(byte) 0x01,0x5F,(byte) 0x90};
			assertArrayEquals(expected6thGasLimit, eblock.getEthereumTransactions().get(transactNum).getGasLimitRaw(),"Transaction 6 has a correct gas limit");
			byte[] expected6thReceiveAddress = new byte[] {(byte)0x1F,(byte)0x57,(byte)0xF8,(byte)0x26,(byte)0xCA,(byte)0xF5,(byte)0x94,(byte)0xF7,(byte)0xA8,(byte)0x37,(byte)0xD9,(byte)0xFC,(byte)0x09,(byte)0x24,(byte)0x56,(byte)0x87,(byte)0x0A,(byte)0x28,(byte)0x93,(byte)0x65};
			assertArrayEquals(expected6thReceiveAddress, eblock.getEthereumTransactions().get(transactNum).getReceiveAddress(),"Transaction 6 has a correct receive address");
			byte[] expected6thValue = new byte[] {0x01,(byte) 0xFE,(byte) 0x81,(byte) 0xC4,(byte) 0xB6,(byte) 0xA0,(byte) 0xD0,0x00};
			assertArrayEquals(expected6thValue, eblock.getEthereumTransactions().get(transactNum).getValueRaw(),"Transaction 6 has a correct value");	
			byte[] expected6thData = new byte[] {};
			assertArrayEquals(expected6thData, eblock.getEthereumTransactions().get(transactNum).getData(),"Transaction 6 has a correct data");
			byte[] expected6thsigv = new byte[] {0x1C};
			assertArrayEquals(expected6thsigv, eblock.getEthereumTransactions().get(transactNum).getSig_v(),"Transaction 6 has a correct sigv");
			byte[] expected6thsigr = new byte[] {(byte)0x0A,(byte)0x4C,(byte)0xA2,(byte)0x18,(byte)0x46,(byte)0x0D,(byte)0xC8,(byte)0x5B,(byte)0x99,(byte)0x07,(byte)0x46,(byte)0xFB,(byte)0xB9,(byte)0x0C,(byte)0x06,(byte)0xF8,(byte)0x25,(byte)0x87,(byte)0x82,(byte)0x80,(byte)0x87,(byte)0x27,(byte)0x98,(byte)0x3C,(byte)0x8B,(byte)0x8D,(byte)0x6A,(byte)0x92,(byte)0x1E,(byte)0x19,(byte)0x9B,(byte)0xCA};
			assertArrayEquals(expected6thsigr, eblock.getEthereumTransactions().get(transactNum).getSig_r(),"Transaction 6 has a correct sigr");
			byte[] expected6thsigs = new byte[] {(byte)0x08,(byte)0xA3,(byte)0xB9,(byte)0xA4,(byte)0x5D,(byte)0x83,(byte)0x1A,(byte)0xC4,(byte)0xAD,(byte)0x37,(byte)0x9D,(byte)0x14,(byte)0xF0,(byte)0xAE,(byte)0x3C,(byte)0x03,(byte)0xC8,(byte)0x73,(byte)0x1C,(byte)0xB4,(byte)0x4D,(byte)0x8A,(byte)0x79,(byte)0xAC,(byte)0xD4,(byte)0xCD,(byte)0x6C,(byte)0xEA,(byte)0x1B,(byte)0x54,(byte)0x80,(byte)0x02};
			assertArrayEquals(expected6thsigs, eblock.getEthereumTransactions().get(transactNum).getSig_s(),"Transaction 6 has a correct sigs");

			
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseBlock1346406AsEthereumBlockDirect() throws IOException, EthereumBlockReadException, ParseException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth1346406.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=true;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			EthereumBlock eblock = ebr.readBlock();
			EthereumBlockHeader eblockHeader = eblock.getEthereumBlockHeader();
			List<EthereumTransaction> eTransactions = eblock.getEthereumTransactions();
			List<EthereumBlockHeader> eUncles = eblock.getUncleHeaders();
			assertEquals( 6, eTransactions.size(),"Block contains 6 transactions");
			assertEquals(0, eUncles.size(),"Block contains 0 uncleHeaders");
			byte[] expectedParentHash = new byte[] {(byte)0xBA,(byte)0x6D,(byte)0xD2,(byte)0x60,(byte)0x12,(byte)0xB3,(byte)0x71,(byte)0x90,(byte)0x48,(byte)0xF3,(byte)0x16,(byte)0xC6,(byte)0xED,(byte)0xB3,(byte)0x34,(byte)0x9B,(byte)0xDF,(byte)0xBD,(byte)0x61,(byte)0x31,(byte)0x9F,(byte)0xA9,(byte)0x7C,(byte)0x61,(byte)0x6A,(byte)0x61,(byte)0x31,(byte)0x18,(byte)0xA1,(byte)0xAF,(byte)0x30,(byte)0x67};
			
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block contains a correct 32 byte parent hash");
			byte[] expectedUncleHash = new byte[] {(byte) 0x1D,(byte) 0xCC,0x4D,(byte) 0xE8,(byte) 0xDE, (byte) 0xC7,(byte) 0x5D,
					(byte) 0x7A,(byte) 0xAB,(byte) 0x85,(byte) 0xB5,(byte) 0x67,(byte) 0xB6,(byte) 0xCC,(byte) 0xD4,
					0x1A,(byte) 0xD3,(byte)0x12, 0x45,0x1B,(byte) 0x94,(byte) 0x8A,0x74,0x13,(byte) 0xF0,
					(byte) 0xA1,0x42,(byte) 0xFD,0x40,(byte) 0xD4,(byte) 0x93,0x47};
			assertArrayEquals( expectedUncleHash, eblockHeader.getUncleHash(),"Block contains a correct 32 byte uncle hash");
			byte[] expectedCoinbase = new byte[] {(byte)0x1A,(byte)0x06,(byte)0x0B,(byte)0x06,(byte)0x04,(byte)0x88,(byte)0x3A,(byte)0x99,(byte)0x80,(byte)0x9E,(byte)0xB3,(byte)0xF7,(byte)0x98,(byte)0xDF,(byte)0x71,(byte)0xBE,(byte)0xF6,(byte)0xC3,(byte)0x58,(byte)0xF1};
			assertArrayEquals(expectedCoinbase,eblockHeader.getCoinBase(),"Block contains a correct  20 byte coinbase");
			byte[] expectedStateRoot= new byte[] {(byte)0x21,(byte)0xBA,(byte)0x88,(byte)0x6F,(byte)0xD2,(byte)0x6F,(byte)0x17,(byte)0xB4,(byte)0x01,(byte)0xF5,(byte)0x39,(byte)0x20,(byte)0x15,(byte)0x33,(byte)0x10,(byte)0xB6,(byte)0x93,(byte)0x9B,(byte)0xAD,(byte)0x8A,(byte)0x5F,(byte)0xC3,(byte)0xBF,(byte)0x8C,(byte)0x50,(byte)0x5C,(byte)0x55,(byte)0x6D,(byte)0xDB,(byte)0xAF,(byte)0xBC,(byte)0x5C};
			assertArrayEquals(expectedStateRoot,eblockHeader.getStateRoot(),"Block contains a correct 32 byte stateroot");
			byte[] expectedTxTrieRoot= new byte[] {(byte)0xB3,(byte)0xCB,(byte)0xC7,(byte)0xF0,(byte)0xD7,(byte)0x87,(byte)0xE5,(byte)0x7D,(byte)0x93,(byte)0x70,(byte)0xB8,(byte)0x02,(byte)0xAB,(byte)0x94,(byte)0x5E,(byte)0x21,(byte)0x99,(byte)0x1C,(byte)0x3E,(byte)0x12,(byte)0x7D,(byte)0x70,(byte)0x12,(byte)0x0C,(byte)0x37,(byte)0xE9,(byte)0xFD,(byte)0xAE,(byte)0x3E,(byte)0xF3,(byte)0xEB,(byte)0xFC};
			assertArrayEquals(expectedTxTrieRoot,eblockHeader.getTxTrieRoot(),"Block contains a correct 32 byte txTrieRoot");	
			byte[] expectedReceiptTrieRoot=new byte[] {(byte)0x9B,(byte)0xCE,(byte)0x71,(byte)0x32,(byte)0xF5,(byte)0x2D,(byte)0x4D,(byte)0x45,(byte)0xA8,(byte)0xA2,(byte)0x47,(byte)0x48,(byte)0x47,(byte)0x86,(byte)0xC7,(byte)0x0B,(byte)0xB2,(byte)0xE6,(byte)0x39,(byte)0x59,(byte)0xC8,(byte)0x56,(byte)0x1B,(byte)0x3A,(byte)0xBF,(byte)0xD4,(byte)0xE7,(byte)0x22,(byte)0xE6,(byte)0x00,(byte)0x6A,(byte)0x27};
			assertArrayEquals(expectedReceiptTrieRoot,eblockHeader.getReceiptTrieRoot(),"Block contains a correct 32 byte ReceiptTrieRoot");
			byte[] expectedLogsBloom = new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
			assertArrayEquals( expectedLogsBloom, eblockHeader.getLogsBloom(),"Block contains a 256 byte log bloom consisting only of 0x00");
			byte[] expectedDifficulty = new byte[] {0x19,(byte) 0xFF,(byte) 0x9E,(byte) 0xC4,0x35,(byte) 0xE0};
	
			assertArrayEquals( expectedDifficulty, eblockHeader.getDifficulty(),"Block contains a correct 5 byte difficulty");
			DateFormat format = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z");
			String expectedDTStr = "16-04-2016 09:34:29 UTC";
			long expectedTimestamp = format.parse(expectedDTStr).getTime() / 1000;
			//1438269988
			assertEquals(expectedTimestamp, eblockHeader.getTimestamp(),"Block contains a timestamp of "+expectedDTStr);
			long expectedNumber = 1346406L;
			assertEquals( BigInteger.valueOf(expectedNumber), eblockHeader.getNumber(),"Block contains a number 1346406");
			byte[] expectedGasLimit = new byte[] {0x47,(byte) 0xE7,(byte) 0xC4}; 
			assertArrayEquals( expectedGasLimit, eblockHeader.getGasLimitRaw(),"Block contains a correct 3 byte gas limit");
			long expectedGasUsed = 126000L;
			assertEquals( BigInteger.valueOf(expectedGasUsed), eblockHeader.getGasUsed(),"Block contains a gas used of  126000");
			byte[] expectedMixHash= new byte[] {(byte)0x4F,(byte)0x57,(byte)0x71,(byte)0xB7,(byte)0x9A,(byte)0x8E,(byte)0x6E,(byte)0x21,(byte)0x99,(byte)0x35,(byte)0x53,(byte)0x9C,(byte)0x47,(byte)0x3E,(byte)0x23,(byte)0xBA,(byte)0xFD,(byte)0x2C,(byte)0xA3,(byte)0x5C,(byte)0xC1,(byte)0x86,(byte)0x20,(byte)0x66,(byte)0x31,(byte)0xC3,(byte)0xB0,(byte)0x9E,(byte)0xD5,(byte)0x76,(byte)0x19,(byte)0x4A};
			assertArrayEquals( expectedMixHash, eblockHeader.getMixHash(),"Block contains a correct 32 byte mix hash");
			byte[] expectedExtraData= new byte[] {(byte)0xD7,(byte)0x83,(byte)0x01,(byte)0x03,(byte)0x05,(byte)0x84,(byte)0x47,(byte)0x65,(byte)0x74,(byte)0x68,(byte)0x87,(byte)0x67,(byte)0x6F,(byte)0x31,(byte)0x2E,(byte)0x35,(byte)0x2E,(byte)0x31,(byte)0x85,(byte)0x6C,(byte)0x69,(byte)0x6E,(byte)0x75,(byte)0x78};
			// corresponds to 010305/Geth/go1.5.1/linux
			assertArrayEquals( expectedExtraData, eblockHeader.getExtraData(),"Block contains correct 24 byte extra data");
			byte[] expectedNonce = new byte[] {(byte)0xFF,(byte)0x7C,(byte)0x7A,(byte)0xEE,(byte)0x0E,(byte)0x88,(byte)0xC5,(byte)0x2D};
			assertArrayEquals( expectedNonce, eblockHeader.getNonce(),"Block contains a correct 8 byte nonce");
			// check transactions
			// 1st transaction
			int transactNum=0;
			byte[] expected1stNonce = new byte[] {0x0c};
			assertArrayEquals(expected1stNonce, eblock.getEthereumTransactions().get(transactNum).getNonce(),"Transaction 1 has a correct nonce");
			byte[] expected1stGasPrice = new byte[] {0x04,(byte) 0xA8,0x17,(byte) 0xC8,0x00};
			assertArrayEquals(expected1stGasPrice, eblock.getEthereumTransactions().get(transactNum).getGasPriceRaw(),"Transaction 1 has a correct gas price");
			byte[] expected1stGasLimit = new byte[] {(byte) 0x52,0x08};
			assertArrayEquals(expected1stGasLimit, eblock.getEthereumTransactions().get(transactNum).getGasLimitRaw(),"Transaction 1 has a correct gas limit");
			byte[] expected1stReceiveAddress = new byte[] {(byte)0x1E,(byte)0x75,(byte)0xF0,(byte)0x2A,(byte)0x6E,(byte)0x9F,(byte)0xF4,(byte)0xFF,(byte)0x16,(byte)0x33,(byte)0x38,(byte)0x25,(byte)0xD9,(byte)0x09,(byte)0xBB,(byte)0x03,(byte)0x33,(byte)0x06,(byte)0xB7,(byte)0x8B};
			assertArrayEquals(expected1stReceiveAddress, eblock.getEthereumTransactions().get(transactNum).getReceiveAddress(),"Transaction 1 has a correct receive address");
			byte[] expected1stValue = new byte[] {0x0E,(byte) 0xD5,(byte) 0xDA,(byte) 0xBC,(byte) 0x91,0x7D,(byte) 0xAC,0x00};
			assertArrayEquals(expected1stValue,eblock.getEthereumTransactions().get(transactNum).getValueRaw(),"Transaction 1 has a correct value");	
			byte[] expected1stData = new byte[] {};
			assertArrayEquals(expected1stData, eblock.getEthereumTransactions().get(transactNum).getData(),"Transaction 1 has a correct data");
			byte[] expected1stsigv = new byte[] {0x1B};
			assertArrayEquals(expected1stsigv, eblock.getEthereumTransactions().get(transactNum).getSig_v(),"Transaction 1 has a correct sigv");
			byte[] expected1stsigr = new byte[] {(byte)0x47,(byte)0xDD,(byte)0xF9,(byte)0x37,(byte)0x68,(byte)0x97,(byte)0x76,(byte)0x78,(byte)0x13,(byte)0x95,(byte)0x5A,(byte)0x9D,(byte)0x46,(byte)0xB6,(byte)0xF1,(byte)0xAA,(byte)0x77,(byte)0x73,(byte)0xE5,(byte)0xC8,(byte)0xC6,(byte)0x21,(byte)0x67,(byte)0x54,(byte)0x1C,(byte)0x80,(byte)0xBF,(byte)0x25,(byte)0x2D,(byte)0xC7,(byte)0xDC,(byte)0xD2};
			assertArrayEquals(expected1stsigr, eblock.getEthereumTransactions().get(transactNum).getSig_r(),"Transaction 1 has a correct sigr");
			byte[] expected1stsigs = new byte[] {(byte)0x0A,(byte)0x31,(byte)0x3F,(byte)0x35,(byte)0x60,(byte)0x32,(byte)0x37,(byte)0x56,(byte)0xB7,(byte)0x28,(byte)0x5F,(byte)0x62,(byte)0x38,(byte)0x51,(byte)0x86,(byte)0x05,(byte)0x82,(byte)0x1A,(byte)0x2B,(byte)0xEE,(byte)0x03,(byte)0x7D,(byte)0xEA,(byte)0x8F,(byte)0x09,(byte)0x22,(byte)0x66,(byte)0x20,(byte)0x89,(byte)0x03,(byte)0x74,(byte)0x59};
			assertArrayEquals(expected1stsigs, eblock.getEthereumTransactions().get(transactNum).getSig_s(),"Transaction 1 has a correct sigs");
			// 2nd transaction
		    transactNum=1;
			byte[] expected2ndNonce = new byte[] {(byte) 0xff,(byte) 0xD7};
			assertArrayEquals(expected2ndNonce, eblock.getEthereumTransactions().get(transactNum).getNonce(),"Transaction 2 has a correct nonce");
			byte[] expected2ndGasPrice = new byte[] {0x04,(byte) 0xA8,0x17,(byte) 0xC8,0x00};
			assertArrayEquals(expected2ndGasPrice, eblock.getEthereumTransactions().get(transactNum).getGasPriceRaw(),"Transaction 2 has a correct gas price");
			byte[] expected2ndGasLimit = new byte[] {(byte) 0x01,0x5F,(byte) 0x90};
			assertArrayEquals(expected2ndGasLimit, eblock.getEthereumTransactions().get(transactNum).getGasLimitRaw(),"Transaction 2 has a correct gas limit");
			byte[] expected2ndReceiveAddress = new byte[] {(byte)0x54,(byte)0x67,(byte)0xFA,(byte)0xBD,(byte)0x30,(byte)0xEB,(byte)0x61,(byte)0xA1,(byte)0x84,(byte)0x61,(byte)0xD1,(byte)0x53,(byte)0xD8,(byte)0xC6,(byte)0xFF,(byte)0xB1,(byte)0x9D,(byte)0xD4,(byte)0x7A,(byte)0x25};
			assertArrayEquals(expected2ndReceiveAddress, eblock.getEthereumTransactions().get(transactNum).getReceiveAddress(),"Transaction 2 has a correct receive address");
			byte[] expected2ndValue = new byte[] {0x46,(byte) 0xEC,(byte) 0x2C,(byte) 0x96,(byte) 0x05,0x0B,(byte) 0x18,0x00};
	
			assertArrayEquals(expected2ndValue, eblock.getEthereumTransactions().get(transactNum).getValueRaw(),"Transaction 2 has a correct value");	
			byte[] expected2ndData = new byte[] {};
			assertArrayEquals(expected2ndData, eblock.getEthereumTransactions().get(transactNum).getData(),"Transaction 2 has a correct data");
			byte[] expected2ndsigv = new byte[] {0x1B};
			assertArrayEquals(expected2ndsigv, eblock.getEthereumTransactions().get(transactNum).getSig_v(),"Transaction 2 has a correct sigv");
			byte[] expected2ndsigr = new byte[] {(byte)0x62,(byte)0x85,(byte)0x3C,(byte)0x63,(byte)0x9A,(byte)0x9B,(byte)0x9E,(byte)0xC4,(byte)0x9B,(byte)0xA9,(byte)0xAC,(byte)0x53,(byte)0xE2,(byte)0x85,(byte)0xB3,(byte)0x4E,(byte)0xD0,(byte)0xB7,(byte)0x65,(byte)0x5C,(byte)0x1B,(byte)0xE3,(byte)0x29,(byte)0xFB,(byte)0x8B,(byte)0x34,(byte)0x70,(byte)0x74,(byte)0x0C,(byte)0x3D,(byte)0x0A,(byte)0x9A};
			assertArrayEquals(expected2ndsigr, eblock.getEthereumTransactions().get(transactNum).getSig_r(),"Transaction 2 has a correct sigr");
			byte[] expected2ndsigs = new byte[] {(byte)0x03,(byte)0xFA,(byte)0xA6,(byte)0xF4,(byte)0xFF,(byte)0x1A,(byte)0x45,(byte)0x76,(byte)0xDF,(byte)0x08,(byte)0x9A,(byte)0x9F,(byte)0x9C,(byte)0xB7,(byte)0x9C,(byte)0xF2,(byte)0xED,(byte)0xC1,(byte)0xC5,(byte)0xBD,(byte)0xEC,(byte)0x0F,(byte)0xE7,(byte)0x9C,(byte)0x79,(byte)0x2A,(byte)0xCB,(byte)0x9E,(byte)0x83,(byte)0xF2,(byte)0x41};
			assertArrayEquals(expected2ndsigs, eblock.getEthereumTransactions().get(transactNum).getSig_s(),"Transaction 2 has a correct sigs");

			// 3rd transaction
		    transactNum=2;
			byte[] expected3rdNonce = new byte[] {0x02,(byte) 0xD7,(byte) 0xDD};
			assertArrayEquals(expected3rdNonce, eblock.getEthereumTransactions().get(transactNum).getNonce(),"Transaction 3 has a correct nonce");
			byte[] expected3rdGasPrice = new byte[] {0x04,(byte) 0xA8,0x17,(byte) 0xC8,0x00};
			assertArrayEquals(expected3rdGasPrice, eblock.getEthereumTransactions().get(transactNum).getGasPriceRaw(),"Transaction 3 has a correct gas price");
			byte[] expected3rdGasLimit = new byte[] {(byte) 0x01,0x5F,(byte) 0x90};
			assertArrayEquals(expected3rdGasLimit, eblock.getEthereumTransactions().get(transactNum).getGasLimitRaw(),"Transaction 3 has a correct gas limit");
			byte[] expected3rdReceiveAddress = new byte[] {(byte)0xB4,(byte)0xD0,(byte)0xCA,(byte)0x2B,(byte)0x7E,(byte)0x4C,(byte)0xB1,(byte)0xE0,(byte)0x61,(byte)0x0D,(byte)0x02,(byte)0x15,(byte)0x4A,(byte)0x10,(byte)0x16,(byte)0x3A,(byte)0xB0,(byte)0xF4,(byte)0x2E,(byte)0x65};
			assertArrayEquals(expected3rdReceiveAddress, eblock.getEthereumTransactions().get(transactNum).getReceiveAddress(),"Transaction 3 has a correct receive address");
			byte[] expected3rdValue = new byte[] {(byte) 0x29,(byte) 0x73,(byte) 0xCD,(byte) 0x62,0x4F,(byte) 0x70,0x00};
			assertArrayEquals(expected3rdValue, eblock.getEthereumTransactions().get(transactNum).getValueRaw(),"Transaction 3 has a correct value");	
			byte[] expected3rdData = new byte[] {};
			assertArrayEquals(expected3rdData, eblock.getEthereumTransactions().get(transactNum).getData(),"Transaction 3 has a correct data");
			byte[] expected3rdsigv = new byte[] {0x1C};
			assertArrayEquals(expected3rdsigv, eblock.getEthereumTransactions().get(transactNum).getSig_v(),"Transaction 3 has a correct sigv");
			byte[] expected3rdsigr = new byte[]  {(byte)0x89,(byte)0xFD,(byte)0x7A,(byte)0x62,(byte)0xCF,(byte)0x44,(byte)0x77,(byte)0xBF,(byte)0xE5,(byte)0xDB,(byte)0xF0,(byte)0xEE,(byte)0xCF,(byte)0x3A,(byte)0x4A,(byte)0x96,(byte)0x71,(byte)0x96,(byte)0x96,(byte)0xFB,(byte)0xBE,(byte)0x16,(byte)0xBA,(byte)0x0A,(byte)0xBA,(byte)0x1D,(byte)0x63,(byte)0x1D,(byte)0x44,(byte)0xC1,(byte)0xEB,(byte)0x58};
			assertArrayEquals(expected3rdsigr, eblock.getEthereumTransactions().get(transactNum).getSig_r(),"Transaction 3 has a correct sigr");
			byte[] expected3rdsigs = new byte[] {(byte)0x24,(byte)0x34,(byte)0x48,(byte)0x64,(byte)0xEB,(byte)0x6A,(byte)0x60,(byte)0xC6,(byte)0x6F,(byte)0xB5,(byte)0xDA,(byte)0xED,(byte)0x02,(byte)0xB5,(byte)0x63,(byte)0x52,(byte)0xE8,(byte)0x17,(byte)0x42,(byte)0x16,(byte)0xB8,(byte)0xA2,(byte)0xD3,(byte)0x33,(byte)0xB7,(byte)0xF3,(byte)0x32,(byte)0xFF,(byte)0x6B,(byte)0xA0,(byte)0x69,(byte)0x9C};
			assertArrayEquals(expected3rdsigs, eblock.getEthereumTransactions().get(transactNum).getSig_s(),"Transaction 3 has a correct sigs");

			// 4th transaction
		    transactNum=3;
			byte[] expected4thNonce = new byte[] {0x02,(byte) 0xD7,(byte) 0xDE};
			assertArrayEquals(expected4thNonce, eblock.getEthereumTransactions().get(transactNum).getNonce(),"Transaction 4 has a correct nonce");
			byte[] expected4thGasPrice = new byte[] {0x04,(byte) 0xA8,0x17,(byte) 0xC8,0x00};
			assertArrayEquals(expected4thGasPrice, eblock.getEthereumTransactions().get(transactNum).getGasPriceRaw(),"Transaction 4 has a correct gas price");
			byte[] expected4thGasLimit = new byte[] {(byte) 0x01,0x5F,(byte) 0x90};
			assertArrayEquals(expected4thGasLimit, eblock.getEthereumTransactions().get(transactNum).getGasLimitRaw(),"Transaction 4 has a correct gas limit");
			byte[] expected4thReceiveAddress = new byte[] {(byte)0x1F,(byte)0x57,(byte)0xF8,(byte)0x26,(byte)0xCA,(byte)0xF5,(byte)0x94,(byte)0xF7,(byte)0xA8,(byte)0x37,(byte)0xD9,(byte)0xFC,(byte)0x09,(byte)0x24,(byte)0x56,(byte)0x87,(byte)0x0A,(byte)0x28,(byte)0x93,(byte)0x65};
			assertArrayEquals(expected4thReceiveAddress, eblock.getEthereumTransactions().get(transactNum).getReceiveAddress(),"Transaction 4 has a correct receive address");
			byte[] expected4thValue = new byte[] {0x01,(byte) 0xD1,(byte) 0x4C,(byte) 0xAC,(byte) 0xFB,0x05,(byte) 0xC4,0x00};
			assertArrayEquals(expected4thValue, eblock.getEthereumTransactions().get(transactNum).getValueRaw(),"Transaction 4 has a correct value");	
			byte[] expected4thData = new byte[] {};
			assertArrayEquals(expected4thData, eblock.getEthereumTransactions().get(transactNum).getData(),"Transaction 4 has a correct data");
			byte[] expected4thsigv = new byte[] {0x1B};
			assertArrayEquals(expected4thsigv, eblock.getEthereumTransactions().get(transactNum).getSig_v(),"Transaction 4 has a correct sigv");
			byte[] expected4thsigr = new byte[] {(byte)0x46,(byte)0x01,(byte)0x57,(byte)0xDC,(byte)0xE4,(byte)0xE9,(byte)0x5D,(byte)0x1D,(byte)0xCC,(byte)0x7A,(byte)0xED,(byte)0x0D,(byte)0x9B,(byte)0x7E,(byte)0x3D,(byte)0x65,(byte)0x37,(byte)0x0C,(byte)0x53,(byte)0xD2,(byte)0x9E,(byte)0xA9,(byte)0xB1,(byte)0xAA,(byte)0x4C,(byte)0x9C,(byte)0x22,(byte)0x14,(byte)0x91,(byte)0x1C,(byte)0xD9,(byte)0x5E};
			assertArrayEquals(expected4thsigr, eblock.getEthereumTransactions().get(transactNum).getSig_r(),"Transaction 4 has a correct sigr");
			byte[] expected4thsigs = new byte[] {(byte)0x6A,(byte)0x84,(byte)0x4F,(byte)0x95,(byte)0x6D,(byte)0x02,(byte)0x46,(byte)0x94,(byte)0x1B,(byte)0x94,(byte)0x30,(byte)0x91,(byte)0x34,(byte)0x21,(byte)0x20,(byte)0xBD,(byte)0x48,(byte)0xE7,(byte)0xC6,(byte)0x35,(byte)0x77,(byte)0xF0,(byte)0xBA,(byte)0x3D,(byte)0x87,(byte)0x59,(byte)0xC9,(byte)0xEC,(byte)0x58,(byte)0x70,(byte)0x4E,(byte)0xEC};
			assertArrayEquals(expected4thsigs, eblock.getEthereumTransactions().get(transactNum).getSig_s(),"Transaction 4 has a correct sigs");

			// 5th transaction
		    transactNum=4;
			byte[] expected5thNonce = new byte[] {0x02,(byte) 0xD7,(byte) 0xDF};
			assertArrayEquals(expected5thNonce, eblock.getEthereumTransactions().get(transactNum).getNonce(),"Transaction 5 has a correct nonce");
			byte[] expected5thGasPrice = new byte[] {0x04,(byte) 0xA8,0x17,(byte) 0xC8,0x00};
			assertArrayEquals(expected5thGasPrice, eblock.getEthereumTransactions().get(transactNum).getGasPriceRaw(),"Transaction 5 has a correct gas price");
			byte[] expected5thGasLimit = new byte[] {(byte) 0x01,0x5F,(byte) 0x90};
			assertArrayEquals(expected5thGasLimit, eblock.getEthereumTransactions().get(transactNum).getGasLimitRaw(),"Transaction 5 has a correct gas limit");
			byte[] expected5thReceiveAddress = new byte[] {(byte)0x1F,(byte)0x57,(byte)0xF8,(byte)0x26,(byte)0xCA,(byte)0xF5,(byte)0x94,(byte)0xF7,(byte)0xA8,(byte)0x37,(byte)0xD9,(byte)0xFC,(byte)0x09,(byte)0x24,(byte)0x56,(byte)0x87,(byte)0x0A,(byte)0x28,(byte)0x93,(byte)0x65};
			assertArrayEquals(expected5thReceiveAddress, eblock.getEthereumTransactions().get(transactNum).getReceiveAddress(),"Transaction 5 has a correct receive address");
			byte[] expected5thValue = new byte[] {0x02,(byte) 0x02,(byte) 0x05,(byte) 0x26,(byte) 0x47,(byte) 0xC0,(byte) 0xF0,0x00};
			assertArrayEquals(expected5thValue, eblock.getEthereumTransactions().get(transactNum).getValueRaw(),"Transaction 5 has a correct value");	
			byte[] expected5thData = new byte[] {};
			assertArrayEquals(expected5thData, eblock.getEthereumTransactions().get(transactNum).getData(),"Transaction 5 has a correct data");
			byte[] expected5thsigv = new byte[] {0x1C};
			assertArrayEquals(expected5thsigv, eblock.getEthereumTransactions().get(transactNum).getSig_v(),"Transaction 5 has a correct sigv");
			byte[] expected5thsigr = new byte[] {(byte)0xE4,(byte)0xBE,(byte)0x97,(byte)0xD5,(byte)0xAF,(byte)0xF1,(byte)0xB5,(byte)0xE7,(byte)0x99,(byte)0x12,(byte)0x96,(byte)0x98,(byte)0x2B,(byte)0xDF,(byte)0xC1,(byte)0xC2,(byte)0x2F,(byte)0x75,(byte)0x21,(byte)0x13,(byte)0x4F,(byte)0x7E,(byte)0x1A,(byte)0x9D,(byte)0xA3,(byte)0x00,(byte)0x42,(byte)0x0D,(byte)0xAD,(byte)0x33,(byte)0x6F,(byte)0x34};
			assertArrayEquals(expected5thsigr, eblock.getEthereumTransactions().get(transactNum).getSig_r(),"Transaction 5 has a correct sigr");
			byte[] expected5thsigs = new byte[] {(byte)0x62,(byte)0xDE,(byte)0xF8,(byte)0xAA,(byte)0x83,(byte)0x65,(byte)0x58,(byte)0xC7,(byte)0xB0,(byte)0xA5,(byte)0x65,(byte)0xB9,(byte)0x7C,(byte)0x9B,(byte)0x27,(byte)0xB2,(byte)0x0E,(byte)0xD9,(byte)0xA0,(byte)0x51,(byte)0xDE,(byte)0x22,(byte)0xAD,(byte)0x8D,(byte)0xBD,(byte)0x62,(byte)0x52,(byte)0x44,(byte)0xCE,(byte)0x64,(byte)0x9E,(byte)0x3D};
			assertArrayEquals(expected5thsigs, eblock.getEthereumTransactions().get(transactNum).getSig_s(),"Transaction 5 has a correct sigs");

			// 6th transaction
		    transactNum=5;
			byte[] expected6thNonce = new byte[] {0x02,(byte) 0xD7,(byte) 0xE0};
			assertArrayEquals(expected6thNonce, eblock.getEthereumTransactions().get(transactNum).getNonce(),"Transaction 6 has a correct nonce");
			byte[] expected6thGasPrice = new byte[] {0x04,(byte) 0xA8,0x17,(byte) 0xC8,0x00};
			assertArrayEquals(expected6thGasPrice,eblock.getEthereumTransactions().get(transactNum).getGasPriceRaw(),"Transaction 6 has a correct gas price");
			byte[] expected6thGasLimit = new byte[] {(byte) 0x01,0x5F,(byte) 0x90};
			assertArrayEquals(expected6thGasLimit, eblock.getEthereumTransactions().get(transactNum).getGasLimitRaw(),"Transaction 6 has a correct gas limit");
			byte[] expected6thReceiveAddress = new byte[] {(byte)0x1F,(byte)0x57,(byte)0xF8,(byte)0x26,(byte)0xCA,(byte)0xF5,(byte)0x94,(byte)0xF7,(byte)0xA8,(byte)0x37,(byte)0xD9,(byte)0xFC,(byte)0x09,(byte)0x24,(byte)0x56,(byte)0x87,(byte)0x0A,(byte)0x28,(byte)0x93,(byte)0x65};
			assertArrayEquals(expected6thReceiveAddress, eblock.getEthereumTransactions().get(transactNum).getReceiveAddress(),"Transaction 6 has a correct receive address");
			byte[] expected6thValue = new byte[] {0x01,(byte) 0xFE,(byte) 0x81,(byte) 0xC4,(byte) 0xB6,(byte) 0xA0,(byte) 0xD0,0x00};
			assertArrayEquals(expected6thValue, eblock.getEthereumTransactions().get(transactNum).getValueRaw(),"Transaction 6 has a correct value");	
			byte[] expected6thData = new byte[] {};
			assertArrayEquals(expected6thData, eblock.getEthereumTransactions().get(transactNum).getData(),"Transaction 6 has a correct data");
			byte[] expected6thsigv = new byte[] {0x1C};
			assertArrayEquals(expected6thsigv, eblock.getEthereumTransactions().get(transactNum).getSig_v(),"Transaction 6 has a correct sigv");
			byte[] expected6thsigr = new byte[] {(byte)0x0A,(byte)0x4C,(byte)0xA2,(byte)0x18,(byte)0x46,(byte)0x0D,(byte)0xC8,(byte)0x5B,(byte)0x99,(byte)0x07,(byte)0x46,(byte)0xFB,(byte)0xB9,(byte)0x0C,(byte)0x06,(byte)0xF8,(byte)0x25,(byte)0x87,(byte)0x82,(byte)0x80,(byte)0x87,(byte)0x27,(byte)0x98,(byte)0x3C,(byte)0x8B,(byte)0x8D,(byte)0x6A,(byte)0x92,(byte)0x1E,(byte)0x19,(byte)0x9B,(byte)0xCA};
			assertArrayEquals(expected6thsigr, eblock.getEthereumTransactions().get(transactNum).getSig_r(),"Transaction 6 has a correct sigr");
			byte[] expected6thsigs = new byte[] {(byte)0x08,(byte)0xA3,(byte)0xB9,(byte)0xA4,(byte)0x5D,(byte)0x83,(byte)0x1A,(byte)0xC4,(byte)0xAD,(byte)0x37,(byte)0x9D,(byte)0x14,(byte)0xF0,(byte)0xAE,(byte)0x3C,(byte)0x03,(byte)0xC8,(byte)0x73,(byte)0x1C,(byte)0xB4,(byte)0x4D,(byte)0x8A,(byte)0x79,(byte)0xAC,(byte)0xD4,(byte)0xCD,(byte)0x6C,(byte)0xEA,(byte)0x1B,(byte)0x54,(byte)0x80,(byte)0x02};
			assertArrayEquals(expected6thsigs, eblock.getEthereumTransactions().get(transactNum).getSig_s(),"Transaction 6 has a correct sigs");

			
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 
	 @Test
	  public void parseBlock3346406AsEthereumBlockHeap() throws IOException, EthereumBlockReadException, ParseException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth3346406.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			EthereumBlock eblock = ebr.readBlock();
			EthereumBlockHeader eblockHeader = eblock.getEthereumBlockHeader();
			List<EthereumTransaction> eTransactions = eblock.getEthereumTransactions();
			List<EthereumBlockHeader> eUncles = eblock.getUncleHeaders();
			assertEquals( 7, eTransactions.size(),"Block contains 7 transactions");
			assertEquals(0, eUncles.size(),"Block contains 0 uncleHeaders");
			byte[] expectedParentHash = new byte[] {(byte)0xD6,(byte)0x56,(byte)0x18,(byte)0x93,(byte)0x7D,(byte)0x7E,(byte)0xC3,(byte)0x21,(byte)0x18,(byte)0x50,(byte)0x09,(byte)0x69,(byte)0xF8,(byte)0xA7,(byte)0xCF,(byte)0xDC,(byte)0xFA,(byte)0xC9,(byte)0x99,(byte)0xF7,(byte)0xCF,(byte)0x80,(byte)0x40,(byte)0x48,(byte)0x84,(byte)0xC0,(byte)0xEF,(byte)0xF8,(byte)0xB8,(byte)0x3B,(byte)0x14,(byte)0xB1};
			
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block contains a correct 32 byte parent hash");
			byte[] expectedUncleHash = new byte[] {(byte) 0x1D,(byte) 0xCC,0x4D,(byte) 0xE8,(byte) 0xDE, (byte) 0xC7,(byte) 0x5D,
					(byte) 0x7A,(byte) 0xAB,(byte) 0x85,(byte) 0xB5,(byte) 0x67,(byte) 0xB6,(byte) 0xCC,(byte) 0xD4,
					0x1A,(byte) 0xD3,(byte)0x12, 0x45,0x1B,(byte) 0x94,(byte) 0x8A,0x74,0x13,(byte) 0xF0,
					(byte) 0xA1,0x42,(byte) 0xFD,0x40,(byte) 0xD4,(byte) 0x93,0x47};
			assertArrayEquals( expectedUncleHash, eblockHeader.getUncleHash(),"Block contains a correct 32 byte uncle hash");
			byte[] expectedCoinbase = new byte[] {(byte)0xEA,(byte)0x67,(byte)0x4F,(byte)0xDD,(byte)0xE7,(byte)0x14,(byte)0xFD,(byte)0x97,(byte)0x9D,(byte)0xE3,(byte)0xED,(byte)0xF0,(byte)0xF5,(byte)0x6A,(byte)0xA9,(byte)0x71,(byte)0x6B,(byte)0x89,(byte)0x8E,(byte)0xC8};
			assertArrayEquals(expectedCoinbase,eblockHeader.getCoinBase(),"Block contains a correct  20 byte coinbase");
			byte[] expectedStateRoot= new byte[] {(byte)0x52,(byte)0x71,(byte)0x93,(byte)0x64,(byte)0xE4,(byte)0x2D,(byte)0xDA,(byte)0x68,(byte)0xAA,(byte)0x74,(byte)0x9E,(byte)0xAE,(byte)0x89,(byte)0x07,(byte)0xF3,(byte)0x1C,(byte)0xFD,(byte)0xF2,(byte)0x9F,(byte)0x00,(byte)0x2A,(byte)0x27,(byte)0x29,(byte)0xA9,(byte)0x68,(byte)0x73,(byte)0x96,(byte)0x40,(byte)0x6B,(byte)0x8A,(byte)0x9C,(byte)0xB2};
			assertArrayEquals(expectedStateRoot,eblockHeader.getStateRoot(),"Block contains a correct 32 byte stateroot");
			byte[] expectedTxTrieRoot= new byte[] {(byte)0x0E,(byte)0x8D,(byte)0x0C,(byte)0xB8,(byte)0x17,(byte)0xEE,(byte)0x96,(byte)0x39,(byte)0x50,(byte)0x13,(byte)0x68,(byte)0x1D,(byte)0x2E,(byte)0x60,(byte)0x56,(byte)0xC5,(byte)0x4F,(byte)0x41,(byte)0x3E,(byte)0xBB,(byte)0xA1,(byte)0x5F,(byte)0x32,(byte)0x14,(byte)0x0D,(byte)0x9A,(byte)0xCB,(byte)0xAA,(byte)0xB2,(byte)0x90,(byte)0xF6,(byte)0x8F};
			assertArrayEquals(expectedTxTrieRoot,eblockHeader.getTxTrieRoot(),"Block contains a correct 32 byte txTrieRoot");	
			byte[] expectedReceiptTrieRoot=new byte[] {(byte)0xD6,(byte)0x8D,(byte)0x99,(byte)0x09,(byte)0x02,(byte)0x7C,(byte)0x74,(byte)0x5B,(byte)0xDB,(byte)0x26,(byte)0xB4,(byte)0x5E,(byte)0xE9,(byte)0x87,(byte)0xD4,(byte)0xFB,(byte)0xAE,(byte)0x8E,(byte)0x29,(byte)0xD5,(byte)0x95,(byte)0xDB,(byte)0x2B,(byte)0x4A,(byte)0x0A,(byte)0x72,(byte)0x83,(byte)0x96,(byte)0xEA,(byte)0x00,(byte)0x12,(byte)0x79};
			assertArrayEquals(expectedReceiptTrieRoot,eblockHeader.getReceiptTrieRoot(),"Block contains a correct 32 byte ReceiptTrieRoot");
			byte[] expectedLogsBloom = new byte[] {(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x80,(byte)0x00,(byte)0x00,(byte)0x02,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x08,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x01,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x40,(byte)0x00,(byte)0x00,(byte)0x01,(byte)0x00,(byte)0x02,(byte)0x80,(byte)0x00,(byte)0x04,(byte)0x02,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x08,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x41,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x02,(byte)0x80,(byte)0x28,(byte)0x20,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x88,(byte)0x00,(byte)0x00,(byte)0x04,(byte)0x00,(byte)0x40,(byte)0x00,(byte)0x00,(byte)0x08,(byte)0x40,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x08,(byte)0x08,(byte)0x00,(byte)0x01,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x04,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x40,(byte)0x01,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x80,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x12,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x04,(byte)0x00,(byte)0xA0,(byte)0x00,(byte)0x40,(byte)0x00,(byte)0x10,(byte)0x00,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x80,(byte)0x00,(byte)0xC0,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x02,(byte)0x40,(byte)0x01,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x90,(byte)0x01,(byte)0x10,(byte)0x02,(byte)0x00,(byte)0x00,(byte)0x04,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x02,(byte)0x00,(byte)0x00,(byte)0x10,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x01,(byte)0x04,(byte)0x01,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x50,(byte)0x80,(byte)0x00,(byte)0x00,(byte)0x80,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x02,(byte)0x40,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x20,(byte)0x00,(byte)0x01,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x50,(byte)0x01,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x80,(byte)0x00,(byte)0x00};
			assertArrayEquals( expectedLogsBloom, eblockHeader.getLogsBloom(),"Block contains a correct 256 byte log bloom");
			byte[] expectedDifficulty = new byte[] {(byte) 0xA7,(byte) 0x83,(byte) 0xB0,(byte) 0xEE,0x72,(byte) 0xC5};

			assertArrayEquals( expectedDifficulty, eblockHeader.getDifficulty(),"Block contains a correct 6 byte difficulty");
			DateFormat format = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z");
			String expectedDTStr = "13-03-2017 21:13:41 UTC";
			long expectedTimestamp = format.parse(expectedDTStr).getTime() / 1000;
			//1438269988
			assertEquals(expectedTimestamp, eblockHeader.getTimestamp(),"Block contains a timestamp of "+expectedDTStr);
			long expectedNumber = 3346406;
			assertEquals( BigInteger.valueOf(expectedNumber), eblockHeader.getNumber(),"Block contains a number 3346406");
			byte[] expectedGasLimit = new byte[] {0x3D,(byte) 0x4C,(byte) 0xEA};
			assertArrayEquals( expectedGasLimit, eblockHeader.getGasLimitRaw(),"Block contains a correct 3 byte gas limit");
			long expectedGasUsed = 1068696L;
			assertEquals( BigInteger.valueOf(expectedGasUsed), eblockHeader.getGasUsed(),"Block contains a gas used of  1068696");
			byte[] expectedMixHash= new byte[] {(byte)0xFE,(byte)0x8C,(byte)0x06,(byte)0x5B,(byte)0x81,(byte)0x17,(byte)0x1F,(byte)0x61,(byte)0x02,(byte)0xE5,(byte)0x66,(byte)0xA0,(byte)0x07,(byte)0x13,(byte)0x3B,(byte)0xF4,(byte)0x0F,(byte)0xF8,(byte)0x08,(byte)0xF9,(byte)0x04,(byte)0x5E,(byte)0x6B,(byte)0x27,(byte)0x52,(byte)0x75,(byte)0xC8,(byte)0xC7,(byte)0x75,(byte)0x07,(byte)0xBE,(byte)0x78};
			assertArrayEquals( expectedMixHash, eblockHeader.getMixHash(),"Block contains a correct 32 byte mix hash");
			byte[] expectedExtraData= new byte[] {(byte)0x65,(byte)0x74,(byte)0x68,(byte)0x65,(byte)0x72,(byte)0x6D,(byte)0x69,(byte)0x6E,(byte)0x65,(byte)0x20,(byte)0x2D,(byte)0x20,(byte)0x41,(byte)0x53,(byte)0x49,(byte)0x41,(byte)0x31};
			// corresponds to ethermine - ASIA1
			assertArrayEquals( expectedExtraData, eblockHeader.getExtraData(),"Block contains correct 24 byte extra data");
		
			byte[] expectedNonce = new byte[] {(byte)0x4F,(byte)0x3C,(byte)0xCB,(byte)0x40,(byte)0x06,(byte)0x0A,(byte)0xC5,(byte)0x97};
			assertArrayEquals( expectedNonce, eblockHeader.getNonce(),"Block contains a correct 8 byte nonce");
			// skip individual transaction check
			
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 @Test
	  public void parseBlock3346406AsEthereumBlockDirect() throws IOException, EthereumBlockReadException, ParseException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth3346406.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=true;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			EthereumBlock eblock = ebr.readBlock();
			EthereumBlockHeader eblockHeader = eblock.getEthereumBlockHeader();
			List<EthereumTransaction> eTransactions = eblock.getEthereumTransactions();
			List<EthereumBlockHeader> eUncles = eblock.getUncleHeaders();
			assertEquals( 7, eTransactions.size(),"Block contains 7 transactions");
			assertEquals(0, eUncles.size(),"Block contains 0 uncleHeaders");
			byte[] expectedParentHash = new byte[] {(byte)0xD6,(byte)0x56,(byte)0x18,(byte)0x93,(byte)0x7D,(byte)0x7E,(byte)0xC3,(byte)0x21,(byte)0x18,(byte)0x50,(byte)0x09,(byte)0x69,(byte)0xF8,(byte)0xA7,(byte)0xCF,(byte)0xDC,(byte)0xFA,(byte)0xC9,(byte)0x99,(byte)0xF7,(byte)0xCF,(byte)0x80,(byte)0x40,(byte)0x48,(byte)0x84,(byte)0xC0,(byte)0xEF,(byte)0xF8,(byte)0xB8,(byte)0x3B,(byte)0x14,(byte)0xB1};
			
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block contains a correct 32 byte parent hash");
			byte[] expectedUncleHash = new byte[] {(byte) 0x1D,(byte) 0xCC,0x4D,(byte) 0xE8,(byte) 0xDE, (byte) 0xC7,(byte) 0x5D,
					(byte) 0x7A,(byte) 0xAB,(byte) 0x85,(byte) 0xB5,(byte) 0x67,(byte) 0xB6,(byte) 0xCC,(byte) 0xD4,
					0x1A,(byte) 0xD3,(byte)0x12, 0x45,0x1B,(byte) 0x94,(byte) 0x8A,0x74,0x13,(byte) 0xF0,
					(byte) 0xA1,0x42,(byte) 0xFD,0x40,(byte) 0xD4,(byte) 0x93,0x47};
			assertArrayEquals( expectedUncleHash, eblockHeader.getUncleHash(),"Block contains a correct 32 byte uncle hash");
			byte[] expectedCoinbase = new byte[] {(byte)0xEA,(byte)0x67,(byte)0x4F,(byte)0xDD,(byte)0xE7,(byte)0x14,(byte)0xFD,(byte)0x97,(byte)0x9D,(byte)0xE3,(byte)0xED,(byte)0xF0,(byte)0xF5,(byte)0x6A,(byte)0xA9,(byte)0x71,(byte)0x6B,(byte)0x89,(byte)0x8E,(byte)0xC8};
			assertArrayEquals(expectedCoinbase,eblockHeader.getCoinBase(),"Block contains a correct  20 byte coinbase");
			byte[] expectedStateRoot= new byte[] {(byte)0x52,(byte)0x71,(byte)0x93,(byte)0x64,(byte)0xE4,(byte)0x2D,(byte)0xDA,(byte)0x68,(byte)0xAA,(byte)0x74,(byte)0x9E,(byte)0xAE,(byte)0x89,(byte)0x07,(byte)0xF3,(byte)0x1C,(byte)0xFD,(byte)0xF2,(byte)0x9F,(byte)0x00,(byte)0x2A,(byte)0x27,(byte)0x29,(byte)0xA9,(byte)0x68,(byte)0x73,(byte)0x96,(byte)0x40,(byte)0x6B,(byte)0x8A,(byte)0x9C,(byte)0xB2};
			assertArrayEquals(expectedStateRoot,eblockHeader.getStateRoot(),"Block contains a correct 32 byte stateroot");
			byte[] expectedTxTrieRoot= new byte[] {(byte)0x0E,(byte)0x8D,(byte)0x0C,(byte)0xB8,(byte)0x17,(byte)0xEE,(byte)0x96,(byte)0x39,(byte)0x50,(byte)0x13,(byte)0x68,(byte)0x1D,(byte)0x2E,(byte)0x60,(byte)0x56,(byte)0xC5,(byte)0x4F,(byte)0x41,(byte)0x3E,(byte)0xBB,(byte)0xA1,(byte)0x5F,(byte)0x32,(byte)0x14,(byte)0x0D,(byte)0x9A,(byte)0xCB,(byte)0xAA,(byte)0xB2,(byte)0x90,(byte)0xF6,(byte)0x8F};
			assertArrayEquals(expectedTxTrieRoot,eblockHeader.getTxTrieRoot(),"Block contains a correct 32 byte txTrieRoot");	
			byte[] expectedReceiptTrieRoot=new byte[] {(byte)0xD6,(byte)0x8D,(byte)0x99,(byte)0x09,(byte)0x02,(byte)0x7C,(byte)0x74,(byte)0x5B,(byte)0xDB,(byte)0x26,(byte)0xB4,(byte)0x5E,(byte)0xE9,(byte)0x87,(byte)0xD4,(byte)0xFB,(byte)0xAE,(byte)0x8E,(byte)0x29,(byte)0xD5,(byte)0x95,(byte)0xDB,(byte)0x2B,(byte)0x4A,(byte)0x0A,(byte)0x72,(byte)0x83,(byte)0x96,(byte)0xEA,(byte)0x00,(byte)0x12,(byte)0x79};
			assertArrayEquals(expectedReceiptTrieRoot,eblockHeader.getReceiptTrieRoot(),"Block contains a correct 32 byte ReceiptTrieRoot");
			byte[] expectedLogsBloom = new byte[] {(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x80,(byte)0x00,(byte)0x00,(byte)0x02,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x08,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x01,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x40,(byte)0x00,(byte)0x00,(byte)0x01,(byte)0x00,(byte)0x02,(byte)0x80,(byte)0x00,(byte)0x04,(byte)0x02,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x08,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x41,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x02,(byte)0x80,(byte)0x28,(byte)0x20,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x88,(byte)0x00,(byte)0x00,(byte)0x04,(byte)0x00,(byte)0x40,(byte)0x00,(byte)0x00,(byte)0x08,(byte)0x40,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x08,(byte)0x08,(byte)0x00,(byte)0x01,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x04,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x40,(byte)0x01,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x80,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x12,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x04,(byte)0x00,(byte)0xA0,(byte)0x00,(byte)0x40,(byte)0x00,(byte)0x10,(byte)0x00,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x80,(byte)0x00,(byte)0xC0,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x02,(byte)0x40,(byte)0x01,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x90,(byte)0x01,(byte)0x10,(byte)0x02,(byte)0x00,(byte)0x00,(byte)0x04,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x02,(byte)0x00,(byte)0x00,(byte)0x10,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x01,(byte)0x04,(byte)0x01,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x50,(byte)0x80,(byte)0x00,(byte)0x00,(byte)0x80,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x10,(byte)0x00,(byte)0x00,(byte)0x02,(byte)0x40,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x20,(byte)0x00,(byte)0x01,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x50,(byte)0x01,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x80,(byte)0x00,(byte)0x00};
			assertArrayEquals( expectedLogsBloom, eblockHeader.getLogsBloom(),"Block contains a correct 256 byte log bloom");
			byte[] expectedDifficulty = new byte[] {(byte) 0xA7,(byte) 0x83,(byte) 0xB0,(byte) 0xEE,0x72,(byte) 0xC5};

			assertArrayEquals( expectedDifficulty, eblockHeader.getDifficulty(),"Block contains a correct 6 byte difficulty");
			DateFormat format = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z");
			String expectedDTStr = "13-03-2017 21:13:41 UTC";
			long expectedTimestamp = format.parse(expectedDTStr).getTime() / 1000;
			//1438269988
			assertEquals(expectedTimestamp, eblockHeader.getTimestamp(),"Block contains a timestamp of "+expectedDTStr);
			long expectedNumber = 3346406;
			assertEquals( BigInteger.valueOf(expectedNumber), eblockHeader.getNumber(),"Block contains a number 3346406");
			byte[] expectedGasLimit = new byte[] {0x3D,(byte) 0x4C,(byte) 0xEA};
			assertArrayEquals( expectedGasLimit, eblockHeader.getGasLimitRaw(),"Block contains a correct 3 byte gas limit");
			long expectedGasUsed = 1068696L;
			assertEquals( BigInteger.valueOf(expectedGasUsed), eblockHeader.getGasUsed(),"Block contains a gas used of  1068696");
			byte[] expectedMixHash= new byte[] {(byte)0xFE,(byte)0x8C,(byte)0x06,(byte)0x5B,(byte)0x81,(byte)0x17,(byte)0x1F,(byte)0x61,(byte)0x02,(byte)0xE5,(byte)0x66,(byte)0xA0,(byte)0x07,(byte)0x13,(byte)0x3B,(byte)0xF4,(byte)0x0F,(byte)0xF8,(byte)0x08,(byte)0xF9,(byte)0x04,(byte)0x5E,(byte)0x6B,(byte)0x27,(byte)0x52,(byte)0x75,(byte)0xC8,(byte)0xC7,(byte)0x75,(byte)0x07,(byte)0xBE,(byte)0x78};
			assertArrayEquals( expectedMixHash, eblockHeader.getMixHash(),"Block contains a correct 32 byte mix hash");
			byte[] expectedExtraData= new byte[] {(byte)0x65,(byte)0x74,(byte)0x68,(byte)0x65,(byte)0x72,(byte)0x6D,(byte)0x69,(byte)0x6E,(byte)0x65,(byte)0x20,(byte)0x2D,(byte)0x20,(byte)0x41,(byte)0x53,(byte)0x49,(byte)0x41,(byte)0x31};
			// corresponds to ethermine - ASIA1
			assertArrayEquals( expectedExtraData, eblockHeader.getExtraData(),"Block contains correct 24 byte extra data");
		
			byte[] expectedNonce = new byte[] {(byte)0x4F,(byte)0x3C,(byte)0xCB,(byte)0x40,(byte)0x06,(byte)0x0A,(byte)0xC5,(byte)0x97};
			assertArrayEquals( expectedNonce, eblockHeader.getNonce(),"Block contains a correct 8 byte nonce");
			// skip individual transaction check
			
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseBlock0to10AsEthereumBlockHeap() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth0to10.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			EthereumBlock eblock = ebr.readBlock();
			EthereumBlockHeader eblockHeader = eblock.getEthereumBlockHeader();
			List<EthereumTransaction> eTransactions = eblock.getEthereumTransactions();
			List<EthereumBlockHeader> eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 0 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 0 contains 0 uncleHeaders");
			byte[] expectedParentHash = new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};		
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 0 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 1 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 1 contains 0 uncleHeaders");
			expectedParentHash = new byte[] {(byte) 0xD4,(byte) 0xE5,0x67,0x40,(byte) 0xF8,0x76,(byte) 0xAE,(byte) 0xF8,(byte) 0xC0,0x10,(byte) 0xB8,0x6A,0x40,(byte) 0xD5,(byte) 0xF5,0x67,0x45,(byte) 0xA1,0x18,(byte) 0xD0,(byte) 0x90,0x6A,0x34,(byte) 0xE6,(byte) 0x9A,(byte) 0xEC,(byte) 0x8C,0x0D,(byte) 0xB1,(byte) 0xCB,(byte) 0x8F,(byte) 0xA3};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 1 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 2 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 2 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x88,(byte)0xe9,(byte)0x6d,(byte)0x45,(byte)0x37,(byte)0xbe,(byte)0xa4,(byte)0xd9,(byte)0xc0,(byte)0x5d,(byte)0x12,(byte)0x54,(byte)0x99,(byte)0x07,(byte)0xb3,(byte)0x25,(byte)0x61,(byte)0xd3,(byte)0xbf,(byte)0x31,(byte)0xf4,(byte)0x5a,(byte)0xae,(byte)0x73,(byte)0x4c,(byte)0xdc,(byte)0x11,(byte)0x9f,(byte)0x13,(byte)0x40,(byte)0x6c,(byte)0xb6};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 2 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 3 contains 0 transactions");
			assertEquals(1, eUncles.size(),"Block 3 contains 1 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0xb4,(byte)0x95,(byte)0xa1,(byte)0xd7,(byte)0xe6,(byte)0x66,(byte)0x31,(byte)0x52,(byte)0xae,(byte)0x92,(byte)0x70,(byte)0x8d,(byte)0xa4,(byte)0x84,(byte)0x33,(byte)0x37,(byte)0xb9,(byte)0x58,(byte)0x14,(byte)0x60,(byte)0x15,(byte)0xa2,(byte)0x80,(byte)0x2f,(byte)0x41,(byte)0x93,(byte)0xa4,(byte)0x10,(byte)0x04,(byte)0x46,(byte)0x98,(byte)0xc9};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 4 contains 0 transactions");
			assertEquals(1, eUncles.size(),"Block 4 contains 1 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x3d,(byte)0x61,(byte)0x22,(byte)0x66,(byte)0x0c,(byte)0xc8,(byte)0x24,(byte)0x37,(byte)0x6f,(byte)0x11,(byte)0xee,(byte)0x84,(byte)0x2f,(byte)0x83,(byte)0xad,(byte)0xdc,(byte)0x35,(byte)0x25,(byte)0xe2,(byte)0xdd,(byte)0x67,(byte)0x56,(byte)0xb9,(byte)0xbc,(byte)0xf0,(byte)0xaf,(byte)0xfa,(byte)0x6a,(byte)0xa8,(byte)0x8c,(byte)0xf7,(byte)0x41};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 4 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 5 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 5 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x23,(byte)0xad,(byte)0xf5,(byte)0xa3,(byte)0xbe,(byte)0x0f,(byte)0x52,(byte)0x35,(byte)0xb3,(byte)0x69,(byte)0x41,(byte)0xbc,(byte)0xb2,(byte)0x9b,(byte)0x62,(byte)0x50,(byte)0x42,(byte)0x78,(byte)0xec,(byte)0x5b,(byte)0x9c,(byte)0xdf,(byte)0xa2,(byte)0x77,(byte)0xb9,(byte)0x92,(byte)0xba,(byte)0x4a,(byte)0x7a,(byte)0x3c,(byte)0xd3,(byte)0xa2};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 5 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 6 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 6 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0xf3,(byte)0x7c,(byte)0x63,(byte)0x2d,(byte)0x36,(byte)0x1e,(byte)0x0a,(byte)0x93,(byte)0xf0,(byte)0x8b,(byte)0xa2,(byte)0x9b,(byte)0x1a,(byte)0x2c,(byte)0x70,(byte)0x8d,(byte)0x9c,(byte)0xaa,(byte)0x3e,(byte)0xe1,(byte)0x9d,(byte)0x1e,(byte)0xe8,(byte)0xd2,(byte)0xa0,(byte)0x26,(byte)0x12,(byte)0xbf,(byte)0xfe,(byte)0x49,(byte)0xf0,(byte)0xa9};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 6 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 7 contains 0 transactions");
			assertEquals(1, eUncles.size(),"Block 7 contains 1 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x1f,(byte)0x1a,(byte)0xed,(byte)0x8e,(byte)0x36,(byte)0x94,(byte)0xa0,(byte)0x67,(byte)0x49,(byte)0x6c,(byte)0x24,(byte)0x8e,(byte)0x61,(byte)0x87,(byte)0x9c,(byte)0xda,(byte)0x99,(byte)0xb0,(byte)0x70,(byte)0x9a,(byte)0x1d,(byte)0xfb,(byte)0xac,(byte)0xd0,(byte)0xb6,(byte)0x93,(byte)0x75,(byte)0x0d,(byte)0xf0,(byte)0x6b,(byte)0x32,(byte)0x6e};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 7 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 8 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 8 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0xe0,(byte)0xc7,(byte)0xc0,(byte)0xb4,(byte)0x6e,(byte)0x11,(byte)0x6b,(byte)0x87,(byte)0x43,(byte)0x54,(byte)0xdc,(byte)0xe6,(byte)0xf6,(byte)0x4b,(byte)0x85,(byte)0x81,(byte)0xbd,(byte)0x23,(byte)0x91,(byte)0x86,(byte)0xb0,(byte)0x3f,(byte)0x30,(byte)0xa9,(byte)0x78,(byte)0xe3,(byte)0xdc,(byte)0x38,(byte)0x65,(byte)0x6f,(byte)0x72,(byte)0x3a};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 8 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 9 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 9 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x2c,(byte)0xe9,(byte)0x43,(byte)0x42,(byte)0xdf,(byte)0x18,(byte)0x6b,(byte)0xab,(byte)0x41,(byte)0x65,(byte)0xc2,(byte)0x68,(byte)0xc4,(byte)0x3a,(byte)0xb9,(byte)0x82,(byte)0xd3,(byte)0x60,(byte)0xc9,(byte)0x47,(byte)0x4f,(byte)0x42,(byte)0x9f,(byte)0xec,(byte)0x55,(byte)0x65,(byte)0xad,(byte)0xfc,(byte)0x5d,(byte)0x1f,(byte)0x25,(byte)0x8b};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 9 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 10 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 10 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x99,(byte)0x7e,(byte)0x47,(byte)0xbf,(byte)0x4c,(byte)0xac,(byte)0x50,(byte)0x9c,(byte)0x62,(byte)0x77,(byte)0x53,(byte)0xc0,(byte)0x63,(byte)0x85,(byte)0xac,(byte)0x86,(byte)0x66,(byte)0x41,(byte)0xec,(byte)0x6f,(byte)0x88,(byte)0x37,(byte)0x34,(byte)0xff,(byte)0x79,(byte)0x44,(byte)0x41,(byte)0x10,(byte)0x00,(byte)0xdc,(byte)0x57,(byte)0x6e};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 10 contains a correct 32 byte parent hash");

			} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseBlock0to10AsEthereumBlockDirect() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth0to10.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=true;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			EthereumBlock eblock = ebr.readBlock();
			EthereumBlockHeader eblockHeader = eblock.getEthereumBlockHeader();
			List<EthereumTransaction> eTransactions = eblock.getEthereumTransactions();
			List<EthereumBlockHeader> eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 0 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 0 contains 0 uncleHeaders");
			byte[] expectedParentHash = new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};		
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 0 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 1 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 1 contains 0 uncleHeaders");
			expectedParentHash = new byte[] {(byte) 0xD4,(byte) 0xE5,0x67,0x40,(byte) 0xF8,0x76,(byte) 0xAE,(byte) 0xF8,(byte) 0xC0,0x10,(byte) 0xB8,0x6A,0x40,(byte) 0xD5,(byte) 0xF5,0x67,0x45,(byte) 0xA1,0x18,(byte) 0xD0,(byte) 0x90,0x6A,0x34,(byte) 0xE6,(byte) 0x9A,(byte) 0xEC,(byte) 0x8C,0x0D,(byte) 0xB1,(byte) 0xCB,(byte) 0x8F,(byte) 0xA3};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 1 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 2 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 2 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x88,(byte)0xe9,(byte)0x6d,(byte)0x45,(byte)0x37,(byte)0xbe,(byte)0xa4,(byte)0xd9,(byte)0xc0,(byte)0x5d,(byte)0x12,(byte)0x54,(byte)0x99,(byte)0x07,(byte)0xb3,(byte)0x25,(byte)0x61,(byte)0xd3,(byte)0xbf,(byte)0x31,(byte)0xf4,(byte)0x5a,(byte)0xae,(byte)0x73,(byte)0x4c,(byte)0xdc,(byte)0x11,(byte)0x9f,(byte)0x13,(byte)0x40,(byte)0x6c,(byte)0xb6};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 2 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 3 contains 0 transactions");
			assertEquals(1, eUncles.size(),"Block 3 contains 1 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0xb4,(byte)0x95,(byte)0xa1,(byte)0xd7,(byte)0xe6,(byte)0x66,(byte)0x31,(byte)0x52,(byte)0xae,(byte)0x92,(byte)0x70,(byte)0x8d,(byte)0xa4,(byte)0x84,(byte)0x33,(byte)0x37,(byte)0xb9,(byte)0x58,(byte)0x14,(byte)0x60,(byte)0x15,(byte)0xa2,(byte)0x80,(byte)0x2f,(byte)0x41,(byte)0x93,(byte)0xa4,(byte)0x10,(byte)0x04,(byte)0x46,(byte)0x98,(byte)0xc9};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 4 contains 0 transactions");
			assertEquals(1, eUncles.size(),"Block 4 contains 1 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x3d,(byte)0x61,(byte)0x22,(byte)0x66,(byte)0x0c,(byte)0xc8,(byte)0x24,(byte)0x37,(byte)0x6f,(byte)0x11,(byte)0xee,(byte)0x84,(byte)0x2f,(byte)0x83,(byte)0xad,(byte)0xdc,(byte)0x35,(byte)0x25,(byte)0xe2,(byte)0xdd,(byte)0x67,(byte)0x56,(byte)0xb9,(byte)0xbc,(byte)0xf0,(byte)0xaf,(byte)0xfa,(byte)0x6a,(byte)0xa8,(byte)0x8c,(byte)0xf7,(byte)0x41};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 4 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 5 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 5 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x23,(byte)0xad,(byte)0xf5,(byte)0xa3,(byte)0xbe,(byte)0x0f,(byte)0x52,(byte)0x35,(byte)0xb3,(byte)0x69,(byte)0x41,(byte)0xbc,(byte)0xb2,(byte)0x9b,(byte)0x62,(byte)0x50,(byte)0x42,(byte)0x78,(byte)0xec,(byte)0x5b,(byte)0x9c,(byte)0xdf,(byte)0xa2,(byte)0x77,(byte)0xb9,(byte)0x92,(byte)0xba,(byte)0x4a,(byte)0x7a,(byte)0x3c,(byte)0xd3,(byte)0xa2};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 5 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 6 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 6 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0xf3,(byte)0x7c,(byte)0x63,(byte)0x2d,(byte)0x36,(byte)0x1e,(byte)0x0a,(byte)0x93,(byte)0xf0,(byte)0x8b,(byte)0xa2,(byte)0x9b,(byte)0x1a,(byte)0x2c,(byte)0x70,(byte)0x8d,(byte)0x9c,(byte)0xaa,(byte)0x3e,(byte)0xe1,(byte)0x9d,(byte)0x1e,(byte)0xe8,(byte)0xd2,(byte)0xa0,(byte)0x26,(byte)0x12,(byte)0xbf,(byte)0xfe,(byte)0x49,(byte)0xf0,(byte)0xa9};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 6 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 7 contains 0 transactions");
			assertEquals(1, eUncles.size(),"Block 7 contains 1 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x1f,(byte)0x1a,(byte)0xed,(byte)0x8e,(byte)0x36,(byte)0x94,(byte)0xa0,(byte)0x67,(byte)0x49,(byte)0x6c,(byte)0x24,(byte)0x8e,(byte)0x61,(byte)0x87,(byte)0x9c,(byte)0xda,(byte)0x99,(byte)0xb0,(byte)0x70,(byte)0x9a,(byte)0x1d,(byte)0xfb,(byte)0xac,(byte)0xd0,(byte)0xb6,(byte)0x93,(byte)0x75,(byte)0x0d,(byte)0xf0,(byte)0x6b,(byte)0x32,(byte)0x6e};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 7 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 8 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 8 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0xe0,(byte)0xc7,(byte)0xc0,(byte)0xb4,(byte)0x6e,(byte)0x11,(byte)0x6b,(byte)0x87,(byte)0x43,(byte)0x54,(byte)0xdc,(byte)0xe6,(byte)0xf6,(byte)0x4b,(byte)0x85,(byte)0x81,(byte)0xbd,(byte)0x23,(byte)0x91,(byte)0x86,(byte)0xb0,(byte)0x3f,(byte)0x30,(byte)0xa9,(byte)0x78,(byte)0xe3,(byte)0xdc,(byte)0x38,(byte)0x65,(byte)0x6f,(byte)0x72,(byte)0x3a};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 8 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 9 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 9 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x2c,(byte)0xe9,(byte)0x43,(byte)0x42,(byte)0xdf,(byte)0x18,(byte)0x6b,(byte)0xab,(byte)0x41,(byte)0x65,(byte)0xc2,(byte)0x68,(byte)0xc4,(byte)0x3a,(byte)0xb9,(byte)0x82,(byte)0xd3,(byte)0x60,(byte)0xc9,(byte)0x47,(byte)0x4f,(byte)0x42,(byte)0x9f,(byte)0xec,(byte)0x55,(byte)0x65,(byte)0xad,(byte)0xfc,(byte)0x5d,(byte)0x1f,(byte)0x25,(byte)0x8b};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 9 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 10 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 10 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x99,(byte)0x7e,(byte)0x47,(byte)0xbf,(byte)0x4c,(byte)0xac,(byte)0x50,(byte)0x9c,(byte)0x62,(byte)0x77,(byte)0x53,(byte)0xc0,(byte)0x63,(byte)0x85,(byte)0xac,(byte)0x86,(byte)0x66,(byte)0x41,(byte)0xec,(byte)0x6f,(byte)0x88,(byte)0x37,(byte)0x34,(byte)0xff,(byte)0x79,(byte)0x44,(byte)0x41,(byte)0x10,(byte)0x00,(byte)0xdc,(byte)0x57,(byte)0x6e};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 10 contains a correct 32 byte parent hash");

			} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 
	 @Test
	  public void parseBlock3510000to3510010AsEthereumBlockHeap() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth351000to3510010.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			EthereumBlock eblock = ebr.readBlock();
			EthereumBlockHeader eblockHeader = eblock.getEthereumBlockHeader();
			List<EthereumTransaction> eTransactions = eblock.getEthereumTransactions();
			List<EthereumBlockHeader> eUncles = eblock.getUncleHeaders();
			assertEquals( 15, eTransactions.size(),"Block 3510000 contains 15 transactions");
			assertEquals(0, eUncles.size(),"Block 3510000 contains 0 uncleHeaders");
			byte[] expectedParentHash = new byte[] {(byte)0x63,(byte)0x74,(byte)0x6f,(byte)0x5b,(byte)0xcf,(byte)0xa3,(byte)0xab,(byte)0x25,(byte)0xda,(byte)0xcf,(byte)0x01,(byte)0xd8,(byte)0x89,(byte)0x50,(byte)0xdc,(byte)0x06,(byte)0x55,(byte)0x6b,(byte)0x8f,(byte)0xd9,(byte)0x30,(byte)0xd6,(byte)0xff,(byte)0xa3,(byte)0x01,(byte)0x31,(byte)0xd7,(byte)0xfe,(byte)0xe6,(byte)0xe1,(byte)0x58,(byte)0x5e};		
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510000 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 3510001 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 3510001 contains 0 uncleHeaders");
			expectedParentHash = new byte[] {(byte)0xc5,(byte)0xaf,(byte)0xd4,(byte)0x24,(byte)0x4a,(byte)0xa5,(byte)0x49,(byte)0x0a,(byte)0x44,(byte)0xad,(byte)0xa6,(byte)0xd4,(byte)0x61,(byte)0x1c,(byte)0x8b,(byte)0xe2,(byte)0x4c,(byte)0x85,(byte)0x0d,(byte)0x83,(byte)0x00,(byte)0x1e,(byte)0xb3,(byte)0xea,(byte)0x98,(byte)0x8a,(byte)0xdc,(byte)0xd5,(byte)0x7c,(byte)0x66,(byte)0x48,(byte)0x7c};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510001 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 18, eTransactions.size(),"Block 3510002 contains 18 transactions");
			assertEquals(0, eUncles.size(),"Block 3510002 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x05,(byte)0x6e,(byte)0x99,(byte)0xc7,(byte)0xde,(byte)0x41,(byte)0x7c,(byte)0x50,(byte)0x4e,(byte)0xcd,(byte)0x61,(byte)0xd7,(byte)0xdd,(byte)0x4e,(byte)0x46,(byte)0x8e,(byte)0xa9,(byte)0xe4,(byte)0x71,(byte)0xd1,(byte)0x35,(byte)0x49,(byte)0x0b,(byte)0x24,(byte)0x51,(byte)0x49,(byte)0xbe,(byte)0x51,(byte)0xfd,(byte)0xa8,(byte)0x46,(byte)0x8a};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510002 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 64, eTransactions.size(),"Block 3510003 contains 64 transactions");
			assertEquals(2, eUncles.size(),"Block 3510003 contains 2 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0xd7,(byte)0x24,(byte)0x0d,(byte)0x4f,(byte)0x3c,(byte)0xc7,(byte)0x6f,(byte)0x70,(byte)0x35,(byte)0x93,(byte)0xcd,(byte)0xce,(byte)0x37,(byte)0x3f,(byte)0x21,(byte)0x07,(byte)0x7c,(byte)0xaf,(byte)0x56,(byte)0x76,(byte)0x06,(byte)0xf5,(byte)0x5a,(byte)0xc9,(byte)0xa8,(byte)0x77,(byte)0x7f,(byte)0x1d,(byte)0xce,(byte)0xd0,(byte)0xec,(byte)0x3c};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510003 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 7, eTransactions.size(),"Block 3510004 contains 7 transactions");
			assertEquals(0, eUncles.size(),"Block 3510004 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0xe5,(byte)0x5e,(byte)0x35,(byte)0x21,(byte)0x50,(byte)0xbd,(byte)0x91,(byte)0xaa,(byte)0xe9,(byte)0x66,(byte)0xdc,(byte)0x5c,(byte)0xb6,(byte)0x16,(byte)0x33,(byte)0x6a,(byte)0xb1,(byte)0x33,(byte)0x05,(byte)0xfb,(byte)0xc5,(byte)0x2d,(byte)0xb6,(byte)0x3b,(byte)0xa4,(byte)0xf6,(byte)0x4f,(byte)0x63,(byte)0xd8,(byte)0x75,(byte)0x88,(byte)0xbd};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510004 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 47, eTransactions.size(),"Block 3510005 contains 47 transactions");
			assertEquals(0, eUncles.size(),"Block 3510005 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x4c,(byte)0x75,(byte)0xc3,(byte)0x33,(byte)0xef,(byte)0xd5,(byte)0x9b,(byte)0x16,(byte)0x76,(byte)0xeb,(byte)0x8b,(byte)0x17,(byte)0xd3,(byte)0xc0,(byte)0x6f,(byte)0xf1,(byte)0x13,(byte)0xed,(byte)0xce,(byte)0xe5,(byte)0x7d,(byte)0xfb,(byte)0x1a,(byte)0x10,(byte)0xf1,(byte)0xff,(byte)0xba,(byte)0x6e,(byte)0x65,(byte)0xb1,(byte)0x1b,(byte)0x0d};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510005 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 44, eTransactions.size(),"Block 3510006 contains 44 transactions");
			assertEquals(0, eUncles.size(),"Block 3510006 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x58,(byte)0xf1,(byte)0x8f,(byte)0x87,(byte)0xc9,(byte)0xe3,(byte)0xfc,(byte)0xc4,(byte)0x8b,(byte)0x65,(byte)0xd9,(byte)0x33,(byte)0xc2,(byte)0x1e,(byte)0xf9,(byte)0x25,(byte)0x16,(byte)0x14,(byte)0x7f,(byte)0xb0,(byte)0xed,(byte)0x4f,(byte)0x6b,(byte)0x2b,(byte)0x91,(byte)0x50,(byte)0x7b,(byte)0x29,(byte)0xc4,(byte)0xfe,(byte)0x45,(byte)0x60};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510006 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 3510007 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 3510007 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x75,(byte)0x04,(byte)0xc7,(byte)0xc8,(byte)0x42,(byte)0xdc,(byte)0x25,(byte)0xcc,(byte)0x5d,(byte)0x5e,(byte)0x6d,(byte)0x7c,(byte)0x29,(byte)0xb0,(byte)0x6c,(byte)0xb7,(byte)0x0f,(byte)0x1c,(byte)0xcd,(byte)0x0f,(byte)0x65,(byte)0xb2,(byte)0x67,(byte)0x47,(byte)0xef,(byte)0x7e,(byte)0xcb,(byte)0xa5,(byte)0x2e,(byte)0x8c,(byte)0x4c,(byte)0x24};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 7 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 7, eTransactions.size(),"Block 3510008 contains 7 transactions");
			assertEquals(0, eUncles.size(),"Block 3510008 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x13,(byte)0xb4,(byte)0xa2,(byte)0xc5,(byte)0xce,(byte)0xf4,(byte)0x80,(byte)0xed,(byte)0xd4,(byte)0x82,(byte)0x8a,(byte)0x6a,(byte)0xb1,(byte)0x46,(byte)0xd2,(byte)0x0b,(byte)0x20,(byte)0x41,(byte)0x0d,(byte)0xda,(byte)0xa4,(byte)0x91,(byte)0xb7,(byte)0xc5,(byte)0x54,(byte)0x34,(byte)0xd0,(byte)0x16,(byte)0x5a,(byte)0x49,(byte)0xa3,(byte)0xe6};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510008 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 6, eTransactions.size(),"Block 3510009 contains 6 transactions");
			assertEquals(0, eUncles.size(),"Block 3510009 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x72,(byte)0x89,(byte)0x48,(byte)0x45,(byte)0x1d,(byte)0x1c,(byte)0xaf,(byte)0xae,(byte)0xad,(byte)0x86,(byte)0xe6,(byte)0x9e,(byte)0x8e,(byte)0x50,(byte)0x21,(byte)0xbf,(byte)0x85,(byte)0xf3,(byte)0x8b,(byte)0x19,(byte)0xe7,(byte)0x0c,(byte)0x73,(byte)0xa2,(byte)0x8f,(byte)0xc7,(byte)0x27,(byte)0x4a,(byte)0x89,(byte)0xa3,(byte)0xbd,(byte)0xba};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510009 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 4, eTransactions.size(),"Block 3510010 contains 4 transactions");
			assertEquals(0, eUncles.size(),"Block 3510010 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0xc5,(byte)0x75,(byte)0xa6,(byte)0x40,(byte)0x4f,(byte)0x86,(byte)0xa4,(byte)0xb6,(byte)0x38,(byte)0x32,(byte)0x79,(byte)0x23,(byte)0x26,(byte)0xc5,(byte)0xb0,(byte)0xf6,(byte)0xf2,(byte)0x5e,(byte)0xa9,(byte)0xd8,(byte)0x83,(byte)0x42,(byte)0xf9,(byte)0xf6,(byte)0xf1,(byte)0xe7,(byte)0xed,(byte)0x21,(byte)0xc8,(byte)0x58,(byte)0x02,(byte)0x38};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510010 contains a correct 32 byte parent hash");

			} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 
	 @Test
	  public void parseBlock3510000to3510010AsEthereumBlockDirect() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth351000to3510010.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=true;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			EthereumBlock eblock = ebr.readBlock();
			EthereumBlockHeader eblockHeader = eblock.getEthereumBlockHeader();
			List<EthereumTransaction> eTransactions = eblock.getEthereumTransactions();
			List<EthereumBlockHeader> eUncles = eblock.getUncleHeaders();
			assertEquals( 15, eTransactions.size(),"Block 3510000 contains 15 transactions");
			assertEquals(0, eUncles.size(),"Block 3510000 contains 0 uncleHeaders");
			byte[] expectedParentHash = new byte[] {(byte)0x63,(byte)0x74,(byte)0x6f,(byte)0x5b,(byte)0xcf,(byte)0xa3,(byte)0xab,(byte)0x25,(byte)0xda,(byte)0xcf,(byte)0x01,(byte)0xd8,(byte)0x89,(byte)0x50,(byte)0xdc,(byte)0x06,(byte)0x55,(byte)0x6b,(byte)0x8f,(byte)0xd9,(byte)0x30,(byte)0xd6,(byte)0xff,(byte)0xa3,(byte)0x01,(byte)0x31,(byte)0xd7,(byte)0xfe,(byte)0xe6,(byte)0xe1,(byte)0x58,(byte)0x5e};		
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510000 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 3510001 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 3510001 contains 0 uncleHeaders");
			expectedParentHash = new byte[] {(byte)0xc5,(byte)0xaf,(byte)0xd4,(byte)0x24,(byte)0x4a,(byte)0xa5,(byte)0x49,(byte)0x0a,(byte)0x44,(byte)0xad,(byte)0xa6,(byte)0xd4,(byte)0x61,(byte)0x1c,(byte)0x8b,(byte)0xe2,(byte)0x4c,(byte)0x85,(byte)0x0d,(byte)0x83,(byte)0x00,(byte)0x1e,(byte)0xb3,(byte)0xea,(byte)0x98,(byte)0x8a,(byte)0xdc,(byte)0xd5,(byte)0x7c,(byte)0x66,(byte)0x48,(byte)0x7c};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510001 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 18, eTransactions.size(),"Block 3510002 contains 18 transactions");
			assertEquals(0, eUncles.size(),"Block 3510002 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x05,(byte)0x6e,(byte)0x99,(byte)0xc7,(byte)0xde,(byte)0x41,(byte)0x7c,(byte)0x50,(byte)0x4e,(byte)0xcd,(byte)0x61,(byte)0xd7,(byte)0xdd,(byte)0x4e,(byte)0x46,(byte)0x8e,(byte)0xa9,(byte)0xe4,(byte)0x71,(byte)0xd1,(byte)0x35,(byte)0x49,(byte)0x0b,(byte)0x24,(byte)0x51,(byte)0x49,(byte)0xbe,(byte)0x51,(byte)0xfd,(byte)0xa8,(byte)0x46,(byte)0x8a};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510002 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 64, eTransactions.size(),"Block 3510003 contains 64 transactions");
			assertEquals(2, eUncles.size(),"Block 3510003 contains 2 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0xd7,(byte)0x24,(byte)0x0d,(byte)0x4f,(byte)0x3c,(byte)0xc7,(byte)0x6f,(byte)0x70,(byte)0x35,(byte)0x93,(byte)0xcd,(byte)0xce,(byte)0x37,(byte)0x3f,(byte)0x21,(byte)0x07,(byte)0x7c,(byte)0xaf,(byte)0x56,(byte)0x76,(byte)0x06,(byte)0xf5,(byte)0x5a,(byte)0xc9,(byte)0xa8,(byte)0x77,(byte)0x7f,(byte)0x1d,(byte)0xce,(byte)0xd0,(byte)0xec,(byte)0x3c};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510003 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 7, eTransactions.size(),"Block 3510004 contains 7 transactions");
			assertEquals(0, eUncles.size(),"Block 3510004 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0xe5,(byte)0x5e,(byte)0x35,(byte)0x21,(byte)0x50,(byte)0xbd,(byte)0x91,(byte)0xaa,(byte)0xe9,(byte)0x66,(byte)0xdc,(byte)0x5c,(byte)0xb6,(byte)0x16,(byte)0x33,(byte)0x6a,(byte)0xb1,(byte)0x33,(byte)0x05,(byte)0xfb,(byte)0xc5,(byte)0x2d,(byte)0xb6,(byte)0x3b,(byte)0xa4,(byte)0xf6,(byte)0x4f,(byte)0x63,(byte)0xd8,(byte)0x75,(byte)0x88,(byte)0xbd};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510004 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 47, eTransactions.size(),"Block 3510005 contains 47 transactions");
			assertEquals(0, eUncles.size(),"Block 3510005 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x4c,(byte)0x75,(byte)0xc3,(byte)0x33,(byte)0xef,(byte)0xd5,(byte)0x9b,(byte)0x16,(byte)0x76,(byte)0xeb,(byte)0x8b,(byte)0x17,(byte)0xd3,(byte)0xc0,(byte)0x6f,(byte)0xf1,(byte)0x13,(byte)0xed,(byte)0xce,(byte)0xe5,(byte)0x7d,(byte)0xfb,(byte)0x1a,(byte)0x10,(byte)0xf1,(byte)0xff,(byte)0xba,(byte)0x6e,(byte)0x65,(byte)0xb1,(byte)0x1b,(byte)0x0d};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510005 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 44, eTransactions.size(),"Block 3510006 contains 44 transactions");
			assertEquals(0, eUncles.size(),"Block 3510006 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x58,(byte)0xf1,(byte)0x8f,(byte)0x87,(byte)0xc9,(byte)0xe3,(byte)0xfc,(byte)0xc4,(byte)0x8b,(byte)0x65,(byte)0xd9,(byte)0x33,(byte)0xc2,(byte)0x1e,(byte)0xf9,(byte)0x25,(byte)0x16,(byte)0x14,(byte)0x7f,(byte)0xb0,(byte)0xed,(byte)0x4f,(byte)0x6b,(byte)0x2b,(byte)0x91,(byte)0x50,(byte)0x7b,(byte)0x29,(byte)0xc4,(byte)0xfe,(byte)0x45,(byte)0x60};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510006 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 0, eTransactions.size(),"Block 3510007 contains 0 transactions");
			assertEquals(0, eUncles.size(),"Block 3510007 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x75,(byte)0x04,(byte)0xc7,(byte)0xc8,(byte)0x42,(byte)0xdc,(byte)0x25,(byte)0xcc,(byte)0x5d,(byte)0x5e,(byte)0x6d,(byte)0x7c,(byte)0x29,(byte)0xb0,(byte)0x6c,(byte)0xb7,(byte)0x0f,(byte)0x1c,(byte)0xcd,(byte)0x0f,(byte)0x65,(byte)0xb2,(byte)0x67,(byte)0x47,(byte)0xef,(byte)0x7e,(byte)0xcb,(byte)0xa5,(byte)0x2e,(byte)0x8c,(byte)0x4c,(byte)0x24};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 7 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 7, eTransactions.size(),"Block 3510008 contains 7 transactions");
			assertEquals(0, eUncles.size(),"Block 3510008 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x13,(byte)0xb4,(byte)0xa2,(byte)0xc5,(byte)0xce,(byte)0xf4,(byte)0x80,(byte)0xed,(byte)0xd4,(byte)0x82,(byte)0x8a,(byte)0x6a,(byte)0xb1,(byte)0x46,(byte)0xd2,(byte)0x0b,(byte)0x20,(byte)0x41,(byte)0x0d,(byte)0xda,(byte)0xa4,(byte)0x91,(byte)0xb7,(byte)0xc5,(byte)0x54,(byte)0x34,(byte)0xd0,(byte)0x16,(byte)0x5a,(byte)0x49,(byte)0xa3,(byte)0xe6};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510008 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 6, eTransactions.size(),"Block 3510009 contains 6 transactions");
			assertEquals(0, eUncles.size(),"Block 3510009 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0x72,(byte)0x89,(byte)0x48,(byte)0x45,(byte)0x1d,(byte)0x1c,(byte)0xaf,(byte)0xae,(byte)0xad,(byte)0x86,(byte)0xe6,(byte)0x9e,(byte)0x8e,(byte)0x50,(byte)0x21,(byte)0xbf,(byte)0x85,(byte)0xf3,(byte)0x8b,(byte)0x19,(byte)0xe7,(byte)0x0c,(byte)0x73,(byte)0xa2,(byte)0x8f,(byte)0xc7,(byte)0x27,(byte)0x4a,(byte)0x89,(byte)0xa3,(byte)0xbd,(byte)0xba};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510009 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
			assertEquals( 4, eTransactions.size(),"Block 3510010 contains 4 transactions");
			assertEquals(0, eUncles.size(),"Block 3510010 contains 0 uncleHeaders");
			expectedParentHash = new byte[]  {(byte)0xc5,(byte)0x75,(byte)0xa6,(byte)0x40,(byte)0x4f,(byte)0x86,(byte)0xa4,(byte)0xb6,(byte)0x38,(byte)0x32,(byte)0x79,(byte)0x23,(byte)0x26,(byte)0xc5,(byte)0xb0,(byte)0xf6,(byte)0xf2,(byte)0x5e,(byte)0xa9,(byte)0xd8,(byte)0x83,(byte)0x42,(byte)0xf9,(byte)0xf6,(byte)0xf1,(byte)0xe7,(byte)0xed,(byte)0x21,(byte)0xc8,(byte)0x58,(byte)0x02,(byte)0x38};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 3510010 contains a correct 32 byte parent hash");

			} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 
	 @Test
	  public void parseBlock7000000to7000010AsEthereumBlockHeap() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth70000007000010";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			EthereumBlock eblock = ebr.readBlock();
			EthereumBlockHeader eblockHeader = eblock.getEthereumBlockHeader();
			List<EthereumTransaction> eTransactions = eblock.getEthereumTransactions();
			List<EthereumBlockHeader> eUncles = eblock.getUncleHeaders();
			assertEquals( 38, eTransactions.size(),"Block 7000000 contains 38 transactions");
			assertEquals(0, eUncles.size(),"Block 7000000 contains 0 uncleHeaders");
			byte[] expectedParentHash = new byte[]{(byte)0xdd,(byte)0xbc,(byte)0xc7,(byte)0xf0,(byte)0x48,(byte)0xf0,(byte)0x7f,(byte)0x5a,(byte)0x53,(byte)0xaa,(byte)0x8a,(byte)0xb2,(byte)0x8d,(byte)0x58,(byte)0x67,(byte)0x8c,(byte)0x4e,(byte)0xe9,(byte)0xba,(byte)0x02,(byte)0x5e,(byte)0x09,(byte)0xc6,(byte)0xb1,(byte)0xcf,(byte)0x43,(byte)0xdf,(byte)0xdc,(byte)0x37,(byte)0x2b,(byte)0x40,(byte)0x31};
			assertArrayEquals( expectedParentHash, eblockHeader.getParentHash(),"Block 7000000 contains a correct 32 byte parent hash");
			eblock = ebr.readBlock();
			eblockHeader = eblock.getEthereumBlockHeader();
			eTransactions = eblock.getEthereumTransactions();
			eUncles = eblock.getUncleHeaders();
	
			} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void issue46ParseBlock4800251AsEthereumBlockHeap() throws IOException, EthereumBlockReadException, ParseException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth4800251.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			EthereumBlock eblock = ebr.readBlock();
			EthereumBlockHeader eblockHeader = eblock.getEthereumBlockHeader();
			List<EthereumTransaction> eTransactions = eblock.getEthereumTransactions();
			List<EthereumBlockHeader> eUncles = eblock.getUncleHeaders();
	
			for (EthereumTransaction currentTransaction: eTransactions) {
				assertTrue(currentTransaction.getGasLimit().compareTo(BigInteger.ZERO)>=0, "Gas limit is positive");
	
				assertTrue(currentTransaction.getGasPrice().compareTo(BigInteger.ZERO)>=0, "Gas price is positive");
	
			}
		}finally {
			if (ebr!=null) {
				ebr.close();
			}
	 	}
	 }
	 
	 
	 @Test
	  public void issue47ParseBlock4800251AsEthereumBlockHeap() throws IOException, EthereumBlockReadException, ParseException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth4800251.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			EthereumBlock eblock = ebr.readBlock();
			EthereumBlockHeader eblockHeader = eblock.getEthereumBlockHeader();
			List<EthereumTransaction> eTransactions = eblock.getEthereumTransactions();
			List<EthereumBlockHeader> eUncles = eblock.getUncleHeaders();
			EthereumTransaction trans3 = eTransactions.get(2);
			assertEquals("B271DFFD8A06F0D2D4DB04DFDB3708C7CFA4F8797E57FC35B18507FA617EA4DE",EthereumUtil.convertByteArrayToHexString(EthereumUtil.getTransactionHash(trans3)),"Transaction 3 has correct hash");
			
		}finally {
			if (ebr!=null) {
				ebr.close();
			}
	 	}
	 }
	 
	 @Test
	  public void issue48ParseBlock4800251AsEthereumBlockHeap() throws IOException, EthereumBlockReadException, ParseException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth4800251.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			EthereumBlock eblock = ebr.readBlock();
			EthereumBlockHeader eblockHeader = eblock.getEthereumBlockHeader();
			List<EthereumTransaction> eTransactions = eblock.getEthereumTransactions();
			List<EthereumBlockHeader> eUncles = eblock.getUncleHeaders();
			EthereumTransaction trans1 = eTransactions.get(0); // this one has a v of 0x25 (37) which indicates EIP-155
			assertEquals("6EB53062EF576DF1C4EB5CA326B866590571BCBD",EthereumUtil.convertByteArrayToHexString(EthereumUtil.getSendAddress(trans1,EthereumFormatReaderTest.CHAIN_ID)),"Transaction 1 has correct sendAddress");
			
		}finally {
			if (ebr!=null) {
				ebr.close();
			}
	 	}
	 }

}
