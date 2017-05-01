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

package org.zuinnote.flink.bitcoin;

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
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinTransaction;
import org.zuinnote.hadoop.bitcoin.format.exception.HadoopCryptoLedgerConfigurationException;

public class FlinkBitcoinDataSourceTest {

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
	    public void checkTestDataMultiBlockAvailable() {
	  	ClassLoader classLoader = getClass().getClassLoader();
	  	String fileName="multiblock.blk";
	  	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	  	assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameGenesis);
	  	File file = new File(fileNameGenesis);
	  	assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
	  	assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
	    }
	   
	 @Test
	 public void restoreState() throws HadoopCryptoLedgerConfigurationException, IOException {
		 // test if state is correctly restored
		 	ClassLoader classLoader = getClass().getClassLoader();
		    String fileName="multiblock.blk";
		    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		    Path file = new Path(fileNameBlock); 
		    FileInputSplit blockInputSplit = new FileInputSplit(0,file,0, -1, null);
		    BitcoinBlockFlinkInputFormat inputFormat = new BitcoinBlockFlinkInputFormat(1024*1024, "F9BEB4D9",false);
		    inputFormat.open(blockInputSplit);
		    assertFalse("End not reached",inputFormat.reachedEnd());
		    BitcoinBlock reuse = new BitcoinBlock();
		    BitcoinBlock nextBlock = inputFormat.nextRecord(reuse);
		    assertNotNull("First Block returned",nextBlock);
		    assertEquals("First block contains exactly one transction",1,nextBlock.getTransactions().size());
		    // save state
		    Long state = inputFormat.getCurrentState();
		    assertEquals("state 293",293,state.longValue());
		    // read 2nd block
		    nextBlock=inputFormat.nextRecord(reuse);
		    assertEquals("Second block contains two transactions",2,nextBlock.getTransactions().size());
		    // restore state
		    inputFormat.reopen(blockInputSplit, state);
		    // read 2nd block again
		    nextBlock=inputFormat.nextRecord(reuse);
		    assertEquals("Second block contains two transactions",2,nextBlock.getTransactions().size());
		    // read 3rd block
		    nextBlock=inputFormat.nextRecord(reuse);
		    assertEquals("Third block contains 343 transactions",343,nextBlock.getTransactions().size());
		    nextBlock=inputFormat.nextRecord(reuse);
		    assertNull("No further block",nextBlock);
		    assertTrue("End reached",inputFormat.reachedEnd());
	 }
	 
	 @Test
	 public void parseBitcoinBlockGenesis() throws HadoopCryptoLedgerConfigurationException, IOException {
		  ClassLoader classLoader = getClass().getClassLoader();
		    String fileName="genesis.blk";
		    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		    Path file = new Path(fileNameBlock); 
		    FileInputSplit blockInputSplit = new FileInputSplit(0,file,0, -1, null);
		    BitcoinBlockFlinkInputFormat inputFormat = new BitcoinBlockFlinkInputFormat(1024*1024, "F9BEB4D9",false);
		    inputFormat.open(blockInputSplit);
		    assertFalse("End not reached",inputFormat.reachedEnd());
		    BitcoinBlock reuse = new BitcoinBlock();
		    BitcoinBlock nextBlock = inputFormat.nextRecord(reuse);
		    assertNotNull("First Block returned",nextBlock);
		    assertEquals("First block contains exactly one transction",1,nextBlock.getTransactions().size());
		    nextBlock=inputFormat.nextRecord(reuse);
		    assertNull("No further block",nextBlock);
		    assertTrue("End reached",inputFormat.reachedEnd());
	 }
	 
	 @Test
	 public void parseBitcoinRawBlock() throws HadoopCryptoLedgerConfigurationException, IOException {
		  ClassLoader classLoader = getClass().getClassLoader();
		    String fileName="genesis.blk";
		    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		    Path file = new Path(fileNameBlock); 
		    FileInputSplit blockInputSplit = new FileInputSplit(0,file,0, -1, null);
		    BitcoinRawBlockFlinkInputFormat inputFormat = new BitcoinRawBlockFlinkInputFormat(1024*1024,"F9BEB4D9",false);
		    inputFormat.open(blockInputSplit);
		    assertFalse("End not reached",inputFormat.reachedEnd());
		    BytesWritable reuse = new BytesWritable();
		    BytesWritable nextBlock = inputFormat.nextRecord(reuse);
		    assertNotNull("First Block returned",nextBlock);
			assertEquals("First Block must have size of 293", 293, nextBlock.getLength());
		    nextBlock=inputFormat.nextRecord(reuse);
		    assertNull("No further block",nextBlock);
		    assertTrue("End reached",inputFormat.reachedEnd());
	 }
	 
	 @Test
	 public void parseBitcoinTransaction() throws HadoopCryptoLedgerConfigurationException, IOException {
		  ClassLoader classLoader = getClass().getClassLoader();
		    String fileName="genesis.blk";
		    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		    Path file = new Path(fileNameBlock); 
		    FileInputSplit blockInputSplit = new FileInputSplit(0,file,0, -1, null);
		    BitcoinTransactionFlinkInputFormat inputFormat = new BitcoinTransactionFlinkInputFormat(1024*1024, "F9BEB4D9",false);
		    inputFormat.open(blockInputSplit);
		    assertFalse("End not reached",inputFormat.reachedEnd());
		    BitcoinTransaction reuse = new BitcoinTransaction();
		    int transactCount=0;
			while (!inputFormat.reachedEnd() && (inputFormat.nextRecord(reuse)!=null)) {
				transactCount++;
			}
			assertEquals("Genesis Block  must contain exactly one transactions", 1, transactCount);
	 }
}