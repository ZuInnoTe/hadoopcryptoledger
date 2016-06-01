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

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;


import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.After;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.nio.ByteBuffer;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;

import org.zuinnote.hadoop.bitcoin.format.BitcoinBlockFileInputFormat;
import org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException;

/**
*
*/

public class BitcoinFormatHadoopTest {
private static JobConf defaultConf = new JobConf();
private static FileSystem localFs = null; 
private static Reporter reporter = Reporter.NULL;

   @BeforeClass
    public static void oneTimeSetUp() throws IOException {
      // one-time initialization code   
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
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
  public void checkTestDataVersion4GzipCompressedBlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version4comp.blk.gz";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
	assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameGenesis);
	File file = new File(fileNameGenesis);
	assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
	assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
  }

 @Test
  public void checkTestDataVersion4Bzip2CompressedBlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version4comp.blk.bz2";
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
  public void readBitcoinRawBlockInputFormatGenesisBlock() throws IOException {
    JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="genesis.blk";
    String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameGenesis);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for genesis block", 1, inputSplits.length);
    	RecordReader<BytesWritable, BytesWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable genesisKey = new BytesWritable();	
	BytesWritable genesisBlock = new BytesWritable();
	assertTrue("Input Split for genesis block contains at least one block", reader.next(genesisKey,genesisBlock));
	assertEquals("Genesis Block must have size of 293", 293, genesisBlock.getLength());
	BytesWritable emptyKey = new BytesWritable();
    	BytesWritable emptyBlock = new BytesWritable();
    	assertFalse("No further blocks in genesis Block", reader.next(emptyKey,emptyBlock));
  }

  @Test
  public void readBitcoinRawBlockInputFormatBlockVersion1() throws IOException {
    JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version1.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for block version 1", 1, inputSplits.length);
    	RecordReader<BytesWritable, BytesWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BytesWritable block = new BytesWritable();
	assertTrue("Input Split for block version contains at least one block", reader.next(key,block));
	assertEquals("Random block version 1  must have size of 482 bytes", 482, block.getLength());
	BytesWritable emptyKey = new BytesWritable();
    	BytesWritable emptyBlock = new BytesWritable();
    	assertFalse("No further blocks in block version 1", reader.next(emptyKey,emptyBlock));
  }


  @Test
  public void readBitcoinRawBlockInputFormatBlockVersion2() throws IOException {
    JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version2.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for block version 2", 1, inputSplits.length);
    	RecordReader<BytesWritable, BytesWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BytesWritable block = new BytesWritable();
	assertTrue("Input Split for block version contains at least one block", reader.next(key,block));
	assertEquals("Random block version 2  must have size of 191.198 bytes", 191198, block.getLength());
	BytesWritable emptyKey = new BytesWritable();
    	BytesWritable emptyBlock = new BytesWritable();
    	assertFalse("No further blocks in block version 2", reader.next(emptyKey,emptyBlock));
  }


  @Test
  public void readBitcoinRawBlockInputFormatBlockVersion3() throws IOException {
    JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version3.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for block version 3", 1, inputSplits.length);
    	RecordReader<BytesWritable, BytesWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BytesWritable block = new BytesWritable();
	assertTrue("Input Split for block version contains at least one block", reader.next(key,block));
	assertEquals("Random block version 3 must have size of 932.199 bytes", 932199, block.getLength());
	BytesWritable emptyKey = new BytesWritable();
    	BytesWritable emptyBlock = new BytesWritable();
    	assertFalse("No further blocks in block version 3", reader.next(emptyKey,emptyBlock));
  }

  @Test
  public void readBitcoinRawBlockInputFormatBlockVersion4() throws IOException {
    JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for block version 4", 1, inputSplits.length);
    	RecordReader<BytesWritable, BytesWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BytesWritable block = new BytesWritable();
	assertTrue("Input Split for block version contains at least one block", reader.next(key,block));
	assertEquals("Random block version 4 must have a size of 998.039 bytes", 998039, block.getLength());
	BytesWritable emptyKey = new BytesWritable();
    	BytesWritable emptyBlock = new BytesWritable();
    	assertFalse("No further blocks in block version 4", reader.next(emptyKey,emptyBlock));
  }

  @Test
  public void readBitcoinRawBlockInputFormatReqSeekBlockVersion1() throws IOException {
    JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="reqseekversion1.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for block requiring seek version 1", 1, inputSplits.length);
    	RecordReader<BytesWritable, BytesWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BytesWritable block = new BytesWritable();
	assertTrue("Input Split for block version contains at least one block", reader.next(key,block));
	assertEquals("Random block requiring seek version 1 must have a size of 482 bytes", 482, block.getLength());
	BytesWritable emptyKey = new BytesWritable();
    	BytesWritable emptyBlock = new BytesWritable();
    	assertFalse("No further blocks in block requiring seek version 1", reader.next(emptyKey,emptyBlock));
  }

  @Test
  public void readBitcoinRawBlockInputFormatMultiBlock() throws IOException {
    JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="multiblock.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for multiblock", 1, inputSplits.length);
    	RecordReader<BytesWritable, BytesWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BytesWritable block = new BytesWritable();
	assertTrue("Input Split for multi block contains the genesis block", reader.next(key,block));
	assertEquals("Genesis Block must have size of 293", 293, block.getLength());
	assertTrue("Input Split for block version contains block version 1", reader.next(key,block));
	assertEquals("Random block version 1  must have size of 482 bytes", 482, block.getLength());
	assertTrue("Input Split for block version contains block version 2", reader.next(key,block));
	assertEquals("Random block version 2  must have size of 191.198 bytes", 191198, block.getLength());
	BytesWritable emptyKey = new BytesWritable();
    	BytesWritable emptyBlock = new BytesWritable();
    	assertFalse("No further blocks in multi block", reader.next(emptyKey,emptyBlock));
  }

  @Test
  public void readBitcoinBlockInputFormatGenesisBlock() throws IOException {
    JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="genesis.blk";
    String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameGenesis);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for genesis block", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinBlock> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable genesisKey = new BytesWritable();	
	BitcoinBlock genesisBlock = new BitcoinBlock();
	assertTrue("Input Split for genesis block contains at least one block", reader.next(genesisKey,genesisBlock));
	assertEquals("Genesis Block must contain exactly one transaction", 1, genesisBlock.getTransactions().length);
	BytesWritable emptyKey = new BytesWritable();
    	BitcoinBlock emptyBlock = new BitcoinBlock();
    	assertFalse("No further blocks in genesis Block", reader.next(emptyKey,emptyBlock));
  }

  @Test
  public void readBitcoinBlockInputFormatBlockVersion1() throws IOException {
    JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version1.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for block version 1", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinBlock> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BitcoinBlock block = new BitcoinBlock();
	assertTrue("Input Split for block version contains at least one block", reader.next(key,block));
	assertEquals("Random block version 1  must contain exactly two transactions", 2, block.getTransactions().length);
	BytesWritable emptyKey = new BytesWritable();
    	BitcoinBlock emptyBlock = new BitcoinBlock();
    	assertFalse("No further blocks in block version 1", reader.next(emptyKey,emptyBlock));
  }


  @Test
  public void readBitcoinBlockInputFormatBlockVersion2() throws IOException {
    JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version2.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for block version 2", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinBlock> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BitcoinBlock block = new BitcoinBlock();
	assertTrue("Input Split for block version contains at least one block", reader.next(key,block));
	assertEquals("Random block version 2  must contain exactly 343 transactions", 343, block.getTransactions().length);
	BytesWritable emptyKey = new BytesWritable();
    	BitcoinBlock emptyBlock = new BitcoinBlock();
    	assertFalse("No further blocks in block version 2", reader.next(emptyKey,emptyBlock));
  }


  @Test
  public void readBitcoinBlockInputFormatBlockVersion3() throws IOException {
    JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version3.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for block version 3", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinBlock> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BitcoinBlock block = new BitcoinBlock();
	assertTrue("Input Split for block version contains at least one block", reader.next(key,block));
	assertEquals("Random block version 3 must contain exactly 1645 transactions", 1645, block.getTransactions().length);
	BytesWritable emptyKey = new BytesWritable();
    	BitcoinBlock emptyBlock = new BitcoinBlock();
    	assertFalse("No further blocks in block version 3", reader.next(emptyKey,emptyBlock));
  }

  @Test
  public void readBitcoinBlockInputFormatBlockVersion4() throws IOException {
    JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for block version 4", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinBlock> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BitcoinBlock block = new BitcoinBlock();
	assertTrue("Input Split for block version contains at least one block", reader.next(key,block));
	assertEquals("Random block version 4 must contain exactly 936 transactions", 936, block.getTransactions().length);
	BytesWritable emptyKey = new BytesWritable();
    	BitcoinBlock emptyBlock = new BitcoinBlock();
    	assertFalse("No further blocks in block version 4", reader.next(emptyKey,emptyBlock));
  }

  @Test
  public void readBitcoinBlockInputFormatReqSeekBlockVersion1() throws IOException {
    JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="reqseekversion1.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for block requiring seek version 1", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinBlock> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BitcoinBlock block = new BitcoinBlock();
	assertTrue("Input Split for block version contains at least one block", reader.next(key,block));
	assertEquals("Random block requiring seek version 1 must contain exactly two transactions", 2, block.getTransactions().length);
	BytesWritable emptyKey = new BytesWritable();
    	BitcoinBlock emptyBlock = new BitcoinBlock();
    	assertFalse("No further blocks in block requiring seek version 1", reader.next(emptyKey,emptyBlock));
  }

  @Test
  public void readBitcoinBlockInputFormatMultiBlock() throws IOException {
    JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="multiblock.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for multiblock", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinBlock> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BitcoinBlock block = new BitcoinBlock();
	assertTrue("Input Split for multi block contains the genesis block", reader.next(key,block));
	assertEquals("Genesis Block must contain exactly one transaction", 1, block.getTransactions().length);
	assertTrue("Input Split for block version contains block version 1", reader.next(key,block));
	assertEquals("Random block version 1  must contain exactly two transactions", 2, block.getTransactions().length);
	assertTrue("Input Split for block version contains at least one block", reader.next(key,block));
	assertEquals("Random block version 2  must contain exactly 343 transactions", 343, block.getTransactions().length);
	BytesWritable emptyKey = new BytesWritable();
    	BitcoinBlock emptyBlock = new BitcoinBlock();
    	assertFalse("No further blocks in multi block", reader.next(emptyKey,emptyBlock));
  }


  @Test
  public void readBitcoinTransactionInputFormatGenesisBlock() throws IOException {
      JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="genesis.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for genesis block", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinTransaction> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BitcoinTransaction transaction = new BitcoinTransaction();
	int transactCount=0;
	while (reader.next(key,transaction)) {
		transactCount++;
	}
	assertEquals("Genesis Block  must contain exactly one transactions", 1, transactCount);
  }

  @Test
  public void readBitcoinTransactionInputFormatBlockVersion1() throws IOException {
      JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version1.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for block version 1", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinTransaction> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BitcoinTransaction transaction = new BitcoinTransaction();
	int transactCount=0;
	while (reader.next(key,transaction)) {
		transactCount++;
	}
	assertEquals("Block version 1 must contain exactly two transactions", 2, transactCount);
  }

  @Test
  public void readBitcoinTransactionInputFormatBlockVersion2() throws IOException {
      JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version2.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for block version 2", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinTransaction> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BitcoinTransaction transaction = new BitcoinTransaction();
	int transactCount=0;
	while (reader.next(key,transaction)) {
		transactCount++;
	}
	assertEquals("Block version 2 must contain exactly 343 transactions", 343, transactCount);
  }

  @Test
  public void readBitcoinTransactionInputFormatBlockVersion3() throws IOException {
      JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version3.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for block version 3", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinTransaction> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BitcoinTransaction transaction = new BitcoinTransaction();
	int transactCount=0;
	while (reader.next(key,transaction)) {
		transactCount++;
	}
	assertEquals("Block version 3 must contain exactly 1645 transactions", 1645, transactCount);
  }

  @Test
  public void readBitcoinTransactionInputFormatBlockVersion4() throws IOException {
      JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for block version 4", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinTransaction> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BitcoinTransaction transaction = new BitcoinTransaction();
	int transactCount=0;
	while (reader.next(key,transaction)) {
		transactCount++;
	}
	assertEquals("Block version 4 must contain exactly 936 transactions", 936, transactCount);
  }

  @Test
  public void readBitcoinTransactionInputFormatBlockVersion1ReqSeek() throws IOException {
      JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="reqseekversion1.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for block version 1 requiring seek", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinTransaction> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BitcoinTransaction transaction = new BitcoinTransaction();
	int transactCount=0;
	while (reader.next(key,transaction)) {
		transactCount++;
	}
	assertEquals("Block version 1 requiring seek must contain exactly two transactions", 2, transactCount);
  }

  @Test
  public void readBitcoinTransactionInputFormatMultiBlock() throws IOException {
      JobConf job = new JobConf(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="multiblock.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for multi block", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinTransaction> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BitcoinTransaction transaction = new BitcoinTransaction();
	int transactCount=0;
	while (reader.next(key,transaction)) {
		transactCount++;
	}
	assertEquals("Multiblock must contain exactly 1+2+343=346 transactions", 346, transactCount);
  }

  @Test
  public void readBitcoinRawBlockInputFormatGzipCompressed() throws IOException {
    JobConf job = new JobConf(defaultConf);
    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, job);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4comp.blk.gz";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for compressed block", 1, inputSplits.length);
    	RecordReader<BytesWritable, BytesWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BytesWritable block = new BytesWritable();
	assertTrue("Input Split for block version contains at least one block", reader.next(key,block));
	assertEquals("Compressed block must have a size of 998.039 bytes", 998039, block.getLength());
	BytesWritable emptyKey = new BytesWritable();
    	BytesWritable emptyBlock = new BytesWritable();
    	assertFalse("No further blocks in compressed block", reader.next(emptyKey,emptyBlock));
  }

  @Test
  public void readBitcoinBlockInputFormatGzipCompressed() throws IOException{
        JobConf job = new JobConf(defaultConf);
    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, job);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4comp.blk.gz";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for compressed block", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinBlock> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BitcoinBlock block = new BitcoinBlock();
	assertTrue("Input Split for block version contains at least one block", reader.next(key,block));
	assertEquals("Compressed block must have at least 936 transactions", 936, block.getTransactions().length);
	BytesWritable emptyKey = new BytesWritable();
    	BitcoinBlock emptyBlock = new BitcoinBlock();
    	assertFalse("No further blocks in compressed block", reader.next(emptyKey,emptyBlock));
  }



  @Test
  public void readBitcoinTransactionInputFormatGzipCompressed() throws IOException{
     JobConf job = new JobConf(defaultConf);
    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, job);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4comp.blk.gz";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for compressed block", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinTransaction> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BitcoinTransaction transaction = new BitcoinTransaction();
	int transactCount=0;
	while (reader.next(key,transaction)) {
		transactCount++;
	}
 	assertEquals("Comrpessed block must have at least 936 transactions", 936, transactCount);
  }



  @Test
  public void readBitcoinRawBlockInputFormatBzip2Compressed() throws IOException {
    JobConf job = new JobConf(defaultConf);
    CompressionCodec bzip2 = new BZip2Codec();
    ReflectionUtils.setConf(bzip2, job);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4comp.blk.bz2";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for compressed block", 1, inputSplits.length);
    	RecordReader<BytesWritable, BytesWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BytesWritable block = new BytesWritable();
	assertTrue("Input Split for block version contains at least one block", reader.next(key,block));
	assertEquals("Compressed block must have a size of 998.039 bytes", 998039, block.getLength());
	BytesWritable emptyKey = new BytesWritable();
    	BytesWritable emptyBlock = new BytesWritable();
    	assertFalse("No further blocks in compressed block", reader.next(emptyKey,emptyBlock));

  }

  @Test
  public void readBitcoinBlockInputFormatBzip2Compressed() throws IOException {
    JobConf job = new JobConf(defaultConf);
    CompressionCodec bzip2 = new BZip2Codec();
    ReflectionUtils.setConf(bzip2, job);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4comp.blk.bz2";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for compressed block", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinBlock> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BitcoinBlock block = new BitcoinBlock();
	assertTrue("Input Split for block version contains at least one block", reader.next(key,block));
	assertEquals("Compressed block must have at least 936 transactions", 936, block.getTransactions().length);
	assertEquals("Compressed block must contain exactly 936 transactions of which the first has one input and script length 4", 4, block.getTransactions()[0].getListOfInputs()[0].getTxInScript().length);
	assertEquals("Compressed block must contain exactly 936 transactions of which the first has two outputs", 2, block.getTransactions()[0].getListOfOutputs().length);
	assertEquals("Compressed block must contain exactly 936 transactions of which the first has two output and the first output script length 25", 25, block.getTransactions()[0].getListOfOutputs()[0].getTxOutScript().length);
	BytesWritable emptyKey = new BytesWritable();
    	BitcoinBlock emptyBlock = new BitcoinBlock();
    	assertFalse("No further blocks in compressed block", reader.next(emptyKey,emptyBlock));
  }


  @Test
  public void readBitcoinTransactionInputFormatBzip2Compressed() throws IOException {
      JobConf job = new JobConf(defaultConf);
    CompressionCodec bzip2 = new BZip2Codec();
    ReflectionUtils.setConf(bzip2, job);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4comp.blk.bz2";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
    format.configure(job);
    InputSplit[] inputSplits = format.getSplits(job,1);
    assertEquals("Only one split generated for compressed block", 1, inputSplits.length);
    	RecordReader<BytesWritable, BitcoinTransaction> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	BytesWritable key = new BytesWritable();	
	BitcoinTransaction transaction = new BitcoinTransaction();
	int transactCount=0;
	while (reader.next(key,transaction)) {
		transactCount++;
	}
 	assertEquals("Comrpessed block must have at least 936 transactions", 936, transactCount);
  }




} 


