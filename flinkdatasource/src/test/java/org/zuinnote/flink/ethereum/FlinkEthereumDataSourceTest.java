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
package org.zuinnote.flink.ethereum;


import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.ethereum.format.common.EthereumBlock;

public class FlinkEthereumDataSourceTest {

	   @BeforeAll
	    public static void oneTimeSetUp() throws IOException {
	      // one-time initialization code   

	    }

	    @AfterAll
	    public static void oneTimeTearDown() {
	        // one-time cleanup code
	      }

	    @BeforeEach
	    public void setUp() {
	    }

	    @AfterEach
	    public void tearDown() {
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
		 public void restoreStateEthereumBlock() throws IOException {
			 // test if state is correctly restored
			 	ClassLoader classLoader = getClass().getClassLoader();
			    String fileName="eth0to10.bin";
			    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
			    Path file = new Path(fileNameBlock); 
			    FileInputSplit blockInputSplit = new FileInputSplit(0,file,0, -1, null);
			    EthereumBlockFlinkInputFormat inputFormat = new EthereumBlockFlinkInputFormat(1024*1024, false);
			    inputFormat.open(blockInputSplit);
			    assertFalse(inputFormat.reachedEnd(),"End not reached");
			    EthereumBlock reuse = new EthereumBlock();
			    EthereumBlock nextBlock = inputFormat.nextRecord(reuse);
			    assertNotNull(nextBlock,"First Block returned");
			    assertEquals(0,nextBlock.getEthereumTransactions().size(),"First block contains 0 transactions");
			    byte[] expectedParentHash = new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};		
				assertArrayEquals( expectedParentHash, nextBlock.getEthereumBlockHeader().getParentHash(),"Block 0 contains a correct 32 byte parent hash");
				
			
			    // save state
			    Long state = inputFormat.getCurrentState();
			    assertEquals(540,state.longValue(),"state 540");
			    // read 2nd block
			    nextBlock=inputFormat.nextRecord(reuse);
				assertEquals( 0, nextBlock.getEthereumTransactions().size(),"Block 1 contains 0 transactions");
				assertEquals(0, nextBlock.getUncleHeaders().size(),"Block 1 contains 0 uncleHeaders");
				expectedParentHash = new byte[] {(byte) 0xD4,(byte) 0xE5,0x67,0x40,(byte) 0xF8,0x76,(byte) 0xAE,(byte) 0xF8,(byte) 0xC0,0x10,(byte) 0xB8,0x6A,0x40,(byte) 0xD5,(byte) 0xF5,0x67,0x45,(byte) 0xA1,0x18,(byte) 0xD0,(byte) 0x90,0x6A,0x34,(byte) 0xE6,(byte) 0x9A,(byte) 0xEC,(byte) 0x8C,0x0D,(byte) 0xB1,(byte) 0xCB,(byte) 0x8F,(byte) 0xA3};
				assertArrayEquals( expectedParentHash, nextBlock.getEthereumBlockHeader().getParentHash(),"Block 1 contains a correct 32 byte parent hash");
			    // restore state
			    inputFormat.reopen(blockInputSplit, state);
			    // read 2nd block again
			    nextBlock=inputFormat.nextRecord(reuse);
				assertEquals( 0, nextBlock.getEthereumTransactions().size(),"Block 1 contains 0 transactions");
				assertEquals(0, nextBlock.getUncleHeaders().size(),"Block 1 contains 0 uncleHeaders");
				expectedParentHash = new byte[] {(byte) 0xD4,(byte) 0xE5,0x67,0x40,(byte) 0xF8,0x76,(byte) 0xAE,(byte) 0xF8,(byte) 0xC0,0x10,(byte) 0xB8,0x6A,0x40,(byte) 0xD5,(byte) 0xF5,0x67,0x45,(byte) 0xA1,0x18,(byte) 0xD0,(byte) 0x90,0x6A,0x34,(byte) 0xE6,(byte) 0x9A,(byte) 0xEC,(byte) 0x8C,0x0D,(byte) 0xB1,(byte) 0xCB,(byte) 0x8F,(byte) 0xA3};
				assertArrayEquals( expectedParentHash, nextBlock.getEthereumBlockHeader().getParentHash(),"Block 1 contains a correct 32 byte parent hash");
			    // read remaing blocks (9)
				int remainingBlockCounter=0;
				while (nextBlock!=null) {					 
					nextBlock=inputFormat.nextRecord(reuse);
					if (nextBlock!=null) {
						remainingBlockCounter++;
					}
				}
				assertEquals(9,remainingBlockCounter,"Remaining blocks 9");
			    assertNull(nextBlock,"No further block");
			    assertTrue(inputFormat.reachedEnd(),"End reached");
		 }
		 
		 @Test
		 public void restoreStateBitcoinRawBlock() throws IOException {
			 // test if state is correctly restored
			 	ClassLoader classLoader = getClass().getClassLoader();
			    String fileName="eth0to10.bin";
			    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
			    Path file = new Path(fileNameBlock); 
			    FileInputSplit blockInputSplit = new FileInputSplit(0,file,0, -1, null);
			    EthereumRawBlockFlinkInputFormat inputFormat = new EthereumRawBlockFlinkInputFormat(1024*1024, false);
			    inputFormat.open(blockInputSplit);
			    assertFalse(inputFormat.reachedEnd(),"End not reached");
			    BytesWritable reuse = new BytesWritable();
			    BytesWritable nextBlock = inputFormat.nextRecord(reuse);
			    assertNotNull(nextBlock,"First Block returned");
			    // save state
			    Long state = inputFormat.getCurrentState();
			    assertEquals(540,state.longValue(),"state 540");
			    // read 2nd block
			    nextBlock=inputFormat.nextRecord(reuse);
			    assertNotNull(nextBlock,"Second block after state save exist");
			    // restore state
			    inputFormat.reopen(blockInputSplit, state);
			    // read 2nd block again
			    nextBlock=inputFormat.nextRecord(reuse);
			    assertNotNull(nextBlock,"Second block after state restore exist");
			    // read remaing blocks (9)
				int remainingBlockCounter=0;
				while (nextBlock!=null) {
					 nextBlock=inputFormat.nextRecord(reuse);
						if (nextBlock!=null) {
							remainingBlockCounter++;
						}
				}
				assertEquals(9,remainingBlockCounter,"Remaining blocks 9");
			    assertNull(nextBlock,"No further block");
			    assertTrue(inputFormat.reachedEnd(),"End reached");
		 }
		 
		
		 
		 @Test
		 public void parseEthereumBlock1346406() throws IOException {
			  ClassLoader classLoader = getClass().getClassLoader();
			    String fileName="eth1346406.bin";
			    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
			    Path file = new Path(fileNameBlock); 
			    FileInputSplit blockInputSplit = new FileInputSplit(0,file,0, -1, null);
			    EthereumBlockFlinkInputFormat inputFormat = new EthereumBlockFlinkInputFormat(1024*1024, false);
			    inputFormat.open(blockInputSplit);
			    assertFalse(inputFormat.reachedEnd(),"End not reached");
			    EthereumBlock reuse = new EthereumBlock();
			    EthereumBlock nextBlock = inputFormat.nextRecord(reuse);
			    assertNotNull(nextBlock,"First Block returned");
			    assertEquals(6,nextBlock.getEthereumTransactions().size(),"First block contains exactly 6 transactions");
			    nextBlock=inputFormat.nextRecord(reuse);
			    assertNull(nextBlock,"No further block");
			    assertTrue(inputFormat.reachedEnd(),"End reached");
		 }
		 
		 @Test
		 public void parseEthereumRawBlock() throws IOException {
			  ClassLoader classLoader = getClass().getClassLoader();
			    String fileName="eth1346406.bin";
			    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
			    Path file = new Path(fileNameBlock); 
			    FileInputSplit blockInputSplit = new FileInputSplit(0,file,0, -1, null);
			    EthereumRawBlockFlinkInputFormat inputFormat = new EthereumRawBlockFlinkInputFormat(1024*1024,false);
			    inputFormat.open(blockInputSplit);
			    assertFalse(inputFormat.reachedEnd(),"End not reached");
			    BytesWritable reuse = new BytesWritable();
			    BytesWritable nextBlock = inputFormat.nextRecord(reuse);
			    assertNotNull(nextBlock,"First Block returned");
				assertEquals( 1223, nextBlock.getLength(),"First Block must have size of 1223");
			    nextBlock=inputFormat.nextRecord(reuse);
			    assertNull(nextBlock,"No further block");
			    assertTrue(inputFormat.reachedEnd(),"End reached");
		 }
		 

}
