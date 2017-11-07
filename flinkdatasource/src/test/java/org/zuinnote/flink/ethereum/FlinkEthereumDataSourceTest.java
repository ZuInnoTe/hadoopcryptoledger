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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zuinnote.hadoop.ethereum.format.common.EthereumBlock;

public class FlinkEthereumDataSourceTest {

	   @BeforeClass
	    public static void oneTimeSetUp() throws IOException {
	      // one-time initialization code   

	    }

	    @AfterClass
	    public static void oneTimeTearDown() {
	        // one-time cleanup code
	      }

	    @Before
	    public void setUp() {
	    }

	    @After
	    public void tearDown() {
		}
	       

		 @Test
		  public void checkTestDataBlock0to10Available() {
			ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth0to10.bin";
			String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
			assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameGenesis);
			File file = new File(fileNameGenesis);
			assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
			assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
		  }
		 
		 @Test
		  public void checkTestDataBlock1346406Available() {
			ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth1346406.bin";
			String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
			assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameGenesis);
			File file = new File(fileNameGenesis);
			assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
			assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
		  }
		 
		 @Test
		  public void checkTestDataBlock3346406Available() {
			ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth3346406.bin";
			String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
			assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameGenesis);
			File file = new File(fileNameGenesis);
			assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
			assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
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
			    assertFalse("End not reached",inputFormat.reachedEnd());
			    EthereumBlock reuse = new EthereumBlock();
			    EthereumBlock nextBlock = inputFormat.nextRecord(reuse);
			    assertNotNull("First Block returned",nextBlock);
			    assertEquals("First block contains 0 transactions",0,nextBlock.getEthereumTransactions().size());
			    byte[] expectedParentHash = new byte[] {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};		
				assertArrayEquals("Block 0 contains a correct 32 byte parent hash", expectedParentHash, nextBlock.getEthereumBlockHeader().getParentHash());
				
			
			    // save state
			    Long state = inputFormat.getCurrentState();
			    assertEquals("state 540",540,state.longValue());
			    // read 2nd block
			    nextBlock=inputFormat.nextRecord(reuse);
				assertEquals("Block 1 contains 0 transactions", 0, nextBlock.getEthereumTransactions().size());
				assertEquals("Block 1 contains 0 uncleHeaders",0, nextBlock.getUncleHeaders().size());
				expectedParentHash = new byte[] {(byte) 0xD4,(byte) 0xE5,0x67,0x40,(byte) 0xF8,0x76,(byte) 0xAE,(byte) 0xF8,(byte) 0xC0,0x10,(byte) 0xB8,0x6A,0x40,(byte) 0xD5,(byte) 0xF5,0x67,0x45,(byte) 0xA1,0x18,(byte) 0xD0,(byte) 0x90,0x6A,0x34,(byte) 0xE6,(byte) 0x9A,(byte) 0xEC,(byte) 0x8C,0x0D,(byte) 0xB1,(byte) 0xCB,(byte) 0x8F,(byte) 0xA3};
				assertArrayEquals("Block 1 contains a correct 32 byte parent hash", expectedParentHash, nextBlock.getEthereumBlockHeader().getParentHash());
			    // restore state
			    inputFormat.reopen(blockInputSplit, state);
			    // read 2nd block again
			    nextBlock=inputFormat.nextRecord(reuse);
				assertEquals("Block 1 contains 0 transactions", 0, nextBlock.getEthereumTransactions().size());
				assertEquals("Block 1 contains 0 uncleHeaders",0, nextBlock.getUncleHeaders().size());
				expectedParentHash = new byte[] {(byte) 0xD4,(byte) 0xE5,0x67,0x40,(byte) 0xF8,0x76,(byte) 0xAE,(byte) 0xF8,(byte) 0xC0,0x10,(byte) 0xB8,0x6A,0x40,(byte) 0xD5,(byte) 0xF5,0x67,0x45,(byte) 0xA1,0x18,(byte) 0xD0,(byte) 0x90,0x6A,0x34,(byte) 0xE6,(byte) 0x9A,(byte) 0xEC,(byte) 0x8C,0x0D,(byte) 0xB1,(byte) 0xCB,(byte) 0x8F,(byte) 0xA3};
				assertArrayEquals("Block 1 contains a correct 32 byte parent hash", expectedParentHash, nextBlock.getEthereumBlockHeader().getParentHash());
			    // read remaing blocks (9)
				int remainingBlockCounter=0;
				while (nextBlock!=null) {					 
					nextBlock=inputFormat.nextRecord(reuse);
					if (nextBlock!=null) {
						remainingBlockCounter++;
					}
				}
				assertEquals("Remaining blocks 9",9,remainingBlockCounter);
			    assertNull("No further block",nextBlock);
			    assertTrue("End reached",inputFormat.reachedEnd());
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
			    assertFalse("End not reached",inputFormat.reachedEnd());
			    BytesWritable reuse = new BytesWritable();
			    BytesWritable nextBlock = inputFormat.nextRecord(reuse);
			    assertNotNull("First Block returned",nextBlock);
			    // save state
			    Long state = inputFormat.getCurrentState();
			    assertEquals("state 540",540,state.longValue());
			    // read 2nd block
			    nextBlock=inputFormat.nextRecord(reuse);
			    assertNotNull("Second block after state save exist",nextBlock);
			    // restore state
			    inputFormat.reopen(blockInputSplit, state);
			    // read 2nd block again
			    nextBlock=inputFormat.nextRecord(reuse);
			    assertNotNull("Second block after state restore exist",nextBlock);
			    // read remaing blocks (9)
				int remainingBlockCounter=0;
				while (nextBlock!=null) {
					 nextBlock=inputFormat.nextRecord(reuse);
						if (nextBlock!=null) {
							remainingBlockCounter++;
						}
				}
				assertEquals("Remaining blocks 9",9,remainingBlockCounter);
			    assertNull("No further block",nextBlock);
			    assertTrue("End reached",inputFormat.reachedEnd());
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
			    assertFalse("End not reached",inputFormat.reachedEnd());
			    EthereumBlock reuse = new EthereumBlock();
			    EthereumBlock nextBlock = inputFormat.nextRecord(reuse);
			    assertNotNull("First Block returned",nextBlock);
			    assertEquals("First block contains exactly 6 transactions",6,nextBlock.getEthereumTransactions().size());
			    nextBlock=inputFormat.nextRecord(reuse);
			    assertNull("No further block",nextBlock);
			    assertTrue("End reached",inputFormat.reachedEnd());
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
			    assertFalse("End not reached",inputFormat.reachedEnd());
			    BytesWritable reuse = new BytesWritable();
			    BytesWritable nextBlock = inputFormat.nextRecord(reuse);
			    assertNotNull("First Block returned",nextBlock);
				assertEquals("First Block must have size of 1223", 1223, nextBlock.getLength());
			    nextBlock=inputFormat.nextRecord(reuse);
			    assertNull("No further block",nextBlock);
			    assertTrue("End reached",inputFormat.reachedEnd());
		 }
		 

}
