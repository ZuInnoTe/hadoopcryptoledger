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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinTransaction;
import org.zuinnote.hadoop.bitcoin.format.exception.HadoopCryptoLedgerConfigurationException;

public class FlinkBitcoinDataSourceTest {

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
	    public void checkTestDataMultiBlockAvailable() {
	  	ClassLoader classLoader = getClass().getClassLoader();
	  	String fileName="multiblock.blk";
	  	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	  	assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
	  	File file = new File(fileNameGenesis);
	  	assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
	  	assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
	    }
	   
	 @Test
	 public void restoreStateBitcoinBlock() throws HadoopCryptoLedgerConfigurationException, IOException {
		 // test if state is correctly restored
		 	ClassLoader classLoader = getClass().getClassLoader();
		    String fileName="multiblock.blk";
		    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		    Path file = new Path(fileNameBlock); 
		    FileInputSplit blockInputSplit = new FileInputSplit(0,file,0, -1, null);
		    BitcoinBlockFlinkInputFormat inputFormat = new BitcoinBlockFlinkInputFormat(1024*1024, "F9BEB4D9",false);
		    inputFormat.open(blockInputSplit);
		    assertFalse(inputFormat.reachedEnd(),"End not reached");
		    BitcoinBlock reuse = new BitcoinBlock();
		    BitcoinBlock nextBlock = inputFormat.nextRecord(reuse);
		    assertNotNull(nextBlock,"First Block returned");
		    assertEquals(1,nextBlock.getTransactions().size(),"First block contains exactly one transction");
		    // save state
		    Long state = inputFormat.getCurrentState();
		    assertEquals(293,state.longValue(),"state 293");
		    // read 2nd block
		    nextBlock=inputFormat.nextRecord(reuse);
		    assertEquals(2,nextBlock.getTransactions().size(),"Second block contains two transactions");
		    // restore state
		    inputFormat.reopen(blockInputSplit, state);
		    // read 2nd block again
		    nextBlock=inputFormat.nextRecord(reuse);
		    assertEquals(2,nextBlock.getTransactions().size(),"Second block contains two transactions");
		    // read 3rd block
		    nextBlock=inputFormat.nextRecord(reuse);
		    assertEquals(343,nextBlock.getTransactions().size(),"Third block contains 343 transactions");
		    nextBlock=inputFormat.nextRecord(reuse);
		    assertNull(nextBlock,"No further block");
		    assertTrue(inputFormat.reachedEnd(),"End reached");
	 }
	 
	 @Test
	 public void restoreStateBitcoinRawBlock() throws HadoopCryptoLedgerConfigurationException, IOException {
		 // test if state is correctly restored
		 	ClassLoader classLoader = getClass().getClassLoader();
		    String fileName="multiblock.blk";
		    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		    Path file = new Path(fileNameBlock); 
		    FileInputSplit blockInputSplit = new FileInputSplit(0,file,0, -1, null);
		    BitcoinRawBlockFlinkInputFormat inputFormat = new BitcoinRawBlockFlinkInputFormat(1024*1024, "F9BEB4D9",false);
		    inputFormat.open(blockInputSplit);
		    assertFalse(inputFormat.reachedEnd(),"End not reached");
		    BytesWritable reuse = new BytesWritable();
		    BytesWritable nextBlock = inputFormat.nextRecord(reuse);
		    assertNotNull(nextBlock,"First Block returned");
		    // save state
		    Long state = inputFormat.getCurrentState();
		    assertEquals(293,state.longValue(),"state 293");
		    // read 2nd block
		    nextBlock=inputFormat.nextRecord(reuse);
		    assertNotNull(nextBlock,"Second block after state save exist");
		    // restore state
		    inputFormat.reopen(blockInputSplit, state);
		    // read 2nd block again
		    nextBlock=inputFormat.nextRecord(reuse);
		    assertNotNull(nextBlock,"Second block after state restore exist");
		    // read 3rd block
		    nextBlock=inputFormat.nextRecord(reuse);
		    assertNotNull(nextBlock,"Third block after state restore exist");
		    nextBlock=inputFormat.nextRecord(reuse);
		    assertNull(nextBlock,"No further block");
		    assertTrue(inputFormat.reachedEnd(),"End reached");
	 }
	 
	 @Test
	 public void restoreStateBitcoinTransaction() throws HadoopCryptoLedgerConfigurationException, IOException {
		 // test if state is correctly restored
		 	ClassLoader classLoader = getClass().getClassLoader();
		    String fileName="multiblock.blk";
		    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		    Path file = new Path(fileNameBlock); 
		    FileInputSplit transactionInputSplit = new FileInputSplit(0,file,0, -1, null);
		    BitcoinTransactionFlinkInputFormat inputFormat = new BitcoinTransactionFlinkInputFormat(1024*1024, "F9BEB4D9",false);
		    inputFormat.open(transactionInputSplit);
		    assertFalse(inputFormat.reachedEnd(),"End not reached");
		    BitcoinTransaction reuse = new BitcoinTransaction();
		    BitcoinTransaction nextTransaction = inputFormat.nextRecord(reuse);
		    assertNotNull(nextTransaction,"First Transaction returned");
		    nextTransaction = inputFormat.nextRecord(reuse);
		    assertNotNull(nextTransaction,"Second Transaction returned");
		    // save state
		    Tuple2<Long,Long> state = inputFormat.getCurrentState();
		    assertEquals(775,(long)state.f0,"state buffer position:  775");
		    assertEquals(1,(long)state.f1,"state transacton position: 1");
		    nextTransaction = inputFormat.nextRecord(reuse);
		    assertNotNull(nextTransaction,"Third Transaction returned after state save");
		    // restore state
		    inputFormat.reopen(transactionInputSplit, state);
		    nextTransaction = inputFormat.nextRecord(reuse);
		    assertNotNull(nextTransaction,"Third Transaction returned after state restore");
		    // further transactions
		    int remainingTransactionCounter=0;
		    while (inputFormat.nextRecord(reuse)!=null) {
		    	remainingTransactionCounter++;
		    }
		    assertEquals(343,remainingTransactionCounter,"Reamining transactions after state restore from block 3: 343");
		    assertTrue(inputFormat.reachedEnd(),"End reached");
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
		    assertFalse(inputFormat.reachedEnd(),"End not reached");
		    BitcoinBlock reuse = new BitcoinBlock();
		    BitcoinBlock nextBlock = inputFormat.nextRecord(reuse);
		    assertNotNull(nextBlock,"First Block returned");
		    assertEquals(1,nextBlock.getTransactions().size(),"First block contains exactly one transction");
		    nextBlock=inputFormat.nextRecord(reuse);
		    assertNull(nextBlock,"No further block");
		    assertTrue(inputFormat.reachedEnd(),"End reached");
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
		    assertFalse(inputFormat.reachedEnd(),"End not reached");
		    BytesWritable reuse = new BytesWritable();
		    BytesWritable nextBlock = inputFormat.nextRecord(reuse);
		    assertNotNull(nextBlock,"First Block returned");
			assertEquals( 293, nextBlock.getLength(),"First Block must have size of 293");
		    nextBlock=inputFormat.nextRecord(reuse);
		    assertNull(nextBlock,"No further block");
		    assertTrue(inputFormat.reachedEnd(),"End reached");
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
		    assertFalse(inputFormat.reachedEnd(),"End not reached");
		    BitcoinTransaction reuse = new BitcoinTransaction();
		    int transactCount=0;
			while (!inputFormat.reachedEnd() && (inputFormat.nextRecord(reuse)!=null)) {
				transactCount++;
			}
			assertEquals( 1, transactCount,"Genesis Block  must contain exactly one transactions");
	 }
}