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

package org.zuinnote.hadoop.bitcoin.format.mapreduce;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;

import java.lang.InterruptedException;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;

import org.zuinnote.hadoop.bitcoin.format.mapreduce.BitcoinBlockFileInputFormat;

import org.zuinnote.hadoop.bitcoin.format.common.*;

/**
*
*/

public class BitcoinFormatHadoopTest {
private static Configuration defaultConf = new Configuration();
private static FileSystem localFs = null;

   @BeforeAll
    public static void oneTimeSetUp() throws IOException {
      // one-time initialization code
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
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
  public void checkTestDataVersion4GzipCompressedBlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version4comp.blk.gz";
	String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();
	assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
	File file = new File(fileNameGenesis);
	assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
	assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
  }

 @Test
  public void checkTestDataVersion4Bzip2CompressedBlockAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="version4comp.blk.bz2";
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
  public void readBitcoinRawBlockInputFormatGenesisBlock() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="genesis.blk";
    String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameGenesis);
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();

    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for genesis block");
    	RecordReader<BytesWritable, BytesWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable genesisKey = new BytesWritable();
	BytesWritable genesisBlock = new BytesWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for genesis block contains at least one block");
	genesisKey=reader.getCurrentKey();
	genesisBlock=reader.getCurrentValue();
	assertEquals( 293, genesisBlock.getLength(),"Genesis Block must have size of 293");
    	assertFalse( reader.nextKeyValue(),"No further blocks in genesis Block");
	reader.close();
  }

  @Test
  public void readBitcoinRawBlockInputFormatBlockVersion1() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version1.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for block version 1");
    	RecordReader<BytesWritable, BytesWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable key = new BytesWritable();
	BytesWritable block = new BytesWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains at least one block");
	block=reader.getCurrentValue();
	assertEquals( 482, block.getLength(),"Random block version 1  must have size of 482 bytes");
    	assertFalse( reader.nextKeyValue(),"No further blocks in block version 1");
	reader.close();
  }


  @Test
  public void readBitcoinRawBlockInputFormatBlockVersion2() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version2.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
	Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for block version 2");
    	RecordReader<BytesWritable, BytesWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable key = new BytesWritable();
	BytesWritable block = new BytesWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains at least one block");
	block=reader.getCurrentValue();
	assertEquals( 191198, block.getLength(),"Random block version 2  must have size of 191.198 bytes");
    	assertFalse( reader.nextKeyValue(),"No further blocks in block version 2");
	reader.close();
  }


  @Test
  public void readBitcoinRawBlockInputFormatBlockVersion3() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version3.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
	Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for block version 3");
    	RecordReader<BytesWritable, BytesWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable key = new BytesWritable();
	BytesWritable block = new BytesWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains at least one block");
	block=reader.getCurrentValue();
	assertEquals( 932199, block.getLength(),"Random block version 3 must have size of 932.199 bytes");
    	assertFalse( reader.nextKeyValue(),"No further blocks in block version 3");
	reader.close();
  }

  @Test
  public void readBitcoinRawBlockInputFormatBlockVersion4() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    	Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();
     List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for block version 4");
    	RecordReader<BytesWritable, BytesWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable key = new BytesWritable();
	BytesWritable block = new BytesWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains at least one block");
	block=reader.getCurrentValue();
	assertEquals( 998039, block.getLength(),"Random block version 4 must have a size of 998.039 bytes");
    	assertFalse( reader.nextKeyValue(),"No further blocks in block version 4");
	reader.close();
  }

  @Test
  public void readBitcoinRawBlockInputFormatReqSeekBlockVersion1() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="reqseekversion1.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
        	Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for block requiring seek version 1");
    	RecordReader<BytesWritable, BytesWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable key = new BytesWritable();
	BytesWritable block = new BytesWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains at least one block");
	block=reader.getCurrentValue();
	assertEquals( 482, block.getLength(),"Random block requiring seek version 1 must have a size of 482 bytes");
    	assertFalse( reader.nextKeyValue(),"No further blocks in block requiring seek version 1");
	reader.close();
  }

  @Test
  public void readBitcoinRawBlockInputFormatMultiBlock() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="multiblock.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for multiblock");
    	RecordReader<BytesWritable, BytesWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable key = new BytesWritable();
	BytesWritable block = new BytesWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for multi block contains the genesis block");
	block=reader.getCurrentValue();
	assertEquals( 293, block.getLength(),"Genesis Block must have size of 293");
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains block version 1");
	block=reader.getCurrentValue();
	assertEquals( 482, block.getLength(),"Random block version 1  must have size of 482 bytes");
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains block version 2");
	block=reader.getCurrentValue();
	assertEquals( 191198, block.getLength(),"Random block version 2  must have size of 191.198 bytes");
    	assertFalse( reader.nextKeyValue(),"No further blocks in multi block");
	reader.close();
  }

  @Test
  public void readBitcoinBlockInputFormatGenesisBlock() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="genesis.blk";
    String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameGenesis);
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for genesis block");
    	RecordReader<BytesWritable, BitcoinBlockWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable genesisKey = new BytesWritable();
	BitcoinBlockWritable genesisBlock = new BitcoinBlockWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for genesis block contains at least one block");
	genesisBlock=reader.getCurrentValue();
	assertEquals( 1, genesisBlock.getTransactions().size(),"Genesis Block must contain exactly one transaction");
    	assertFalse( reader.nextKeyValue(),"No further blocks in genesis Block");
	reader.close();
  }

  @Test
  public void readBitcoinBlockInputFormatBlockVersion1() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version1.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for block version 1");
    	RecordReader<BytesWritable, BitcoinBlockWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable key = new BytesWritable();
	BitcoinBlockWritable block = new BitcoinBlockWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains at least one block");
	block=reader.getCurrentValue();
	assertEquals( 2, block.getTransactions().size(),"Random block version 1  must contain exactly two transactions");
    	assertFalse( reader.nextKeyValue(),"No further blocks in block version 1");
	reader.close();
  }


  @Test
  public void readBitcoinBlockInputFormatBlockVersion2() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version2.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for block version 2");
    	RecordReader<BytesWritable, BitcoinBlockWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable key = new BytesWritable();
	BitcoinBlockWritable block = new BitcoinBlockWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains at least one block");
	block=reader.getCurrentValue();
	assertEquals( 343, block.getTransactions().size(),"Random block version 2  must contain exactly 343 transactions");
    	assertFalse( reader.nextKeyValue(),"No further blocks in block version 2");
	reader.close();
  }


  @Test
  public void readBitcoinBlockInputFormatBlockVersion3() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version3.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1,splits.size(),"Only one split generated for block version 3");
    	RecordReader<BytesWritable, BitcoinBlockWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable key = new BytesWritable();
	BitcoinBlockWritable block = new BitcoinBlockWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains at least one block");
	block=reader.getCurrentValue();
	assertEquals( 1645, block.getTransactions().size(),"Random block version 3 must contain exactly 1645 transactions");
    	assertFalse( reader.nextKeyValue(),"No further blocks in block version 3");
	reader.close();
  }

  @Test
  public void readBitcoinBlockInputFormatBlockVersion4() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
   List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for block version 4");
    	RecordReader<BytesWritable, BitcoinBlockWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable key = new BytesWritable();
	BitcoinBlockWritable block = new BitcoinBlockWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains at least one block");
	block=reader.getCurrentValue();
	assertEquals( 936, block.getTransactions().size(),"Random block version 4 must contain exactly 936 transactions");
    	assertFalse( reader.nextKeyValue(),"No further blocks in block version 4");
	reader.close();
  }

  @Test
  public void readBitcoinBlockInputFormatReqSeekBlockVersion1() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="reqseekversion1.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
   List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for block requiring seek version 1");
    	RecordReader<BytesWritable, BitcoinBlockWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable key = new BytesWritable();
	BitcoinBlockWritable block = new BitcoinBlockWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains at least one block");
	block=reader.getCurrentValue();
	assertEquals( 2, block.getTransactions().size(),"Random block requiring seek version 1 must contain exactly two transactions");
    	assertFalse( reader.nextKeyValue(),"No further blocks in block requiring seek version 1");
	reader.close();
  }

  @Test
  public void readBitcoinBlockInputFormatMultiBlock() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="multiblock.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
        Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
  List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1,splits.size(),"Only one split generated for multiblock");
    	RecordReader<BytesWritable, BitcoinBlockWritable> reader = format.createRecordReader(splits.get(0),context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable key = new BytesWritable();
	BitcoinBlockWritable block = new BitcoinBlockWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for multi block contains the genesis block");
	block=reader.getCurrentValue();
	assertEquals( 1, block.getTransactions().size(),"Genesis Block must contain exactly one transaction");
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains block version 1");
	block=reader.getCurrentValue();
	assertEquals( 2, block.getTransactions().size(),"Random block version 1  must contain exactly two transactions");
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains at least one block");
	block=reader.getCurrentValue();
	assertEquals( 343, block.getTransactions().size(),"Random block version 2  must contain exactly 343 transactions");
    	assertFalse( reader.nextKeyValue(),"No further blocks in multi block");
	reader.close();
  }


  @Test
  public void readBitcoinTransactionInputFormatGenesisBlock() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="genesis.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
  List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for genesis block");
    	RecordReader<BytesWritable, BitcoinTransactionWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	int transactCount=0;
	while (reader.nextKeyValue()) {
		transactCount++;
	}
	assertEquals( 1, transactCount,"Genesis Block  must contain exactly one transactions");
	reader.close();
  }

  @Test
  public void readBitcoinTransactionInputFormatBlockVersion1() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version1.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
     List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for block version 1");
    	RecordReader<BytesWritable, BitcoinTransactionWritable> reader = format.createRecordReader(splits.get(0),context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	int transactCount=0;
	while (reader.nextKeyValue()) {
		transactCount++;
	}
	assertEquals( 2, transactCount,"Block version 1 must contain exactly two transactions");
	reader.close();
  }

  @Test
  public void readBitcoinTransactionInputFormatBlockVersion2() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version2.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
     List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for block version 2");
    	RecordReader<BytesWritable, BitcoinTransactionWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	int transactCount=0;
	while (reader.nextKeyValue()) {
		transactCount++;
	}
	assertEquals( 343, transactCount,"Block version 2 must contain exactly 343 transactions");
	reader.close();
  }

  @Test
  public void readBitcoinTransactionInputFormatBlockVersion3() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version3.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
     List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for block version 3");
    	RecordReader<BytesWritable, BitcoinTransactionWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	int transactCount=0;
	while (reader.nextKeyValue()) {
		transactCount++;
	}
	assertEquals( 1645, transactCount,"Block version 3 must contain exactly 1645 transactions");
	reader.close();
  }

  @Test
  public void readBitcoinTransactionInputFormatBlockVersion4() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
     List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for block version 4");
    	RecordReader<BytesWritable, BitcoinTransactionWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	int transactCount=0;
	while (reader.nextKeyValue()) {
		transactCount++;
	}
	assertEquals( 936, transactCount,"Block version 4 must contain exactly 936 transactions");
	reader.close();
  }

  @Test
  public void readBitcoinTransactionInputFormatBlockVersion1ReqSeek() throws IOException, InterruptedException {
   Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="reqseekversion1.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for block version 1 requiring seek");
    	RecordReader<BytesWritable, BitcoinTransactionWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	int transactCount=0;
	while (reader.nextKeyValue()) {
		transactCount++;
	}
	assertEquals( 2, transactCount,"Block version 1 requiring seek must contain exactly two transactions");
	reader.close();
  }

  @Test
  public void readBitcoinTransactionInputFormatMultiBlock() throws IOException, InterruptedException {
   Configuration conf = new Configuration(defaultConf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="multiblock.blk";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for multi block");
    	RecordReader<BytesWritable, BitcoinTransactionWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	int transactCount=0;
	while (reader.nextKeyValue()) {
		transactCount++;
	}
	assertEquals( 346, transactCount,"Multiblock must contain exactly 1+2+343=346 transactions");
	reader.close();
  }

  @Test
  public void readBitcoinRawBlockInputFormatGzipCompressed() throws IOException, InterruptedException {
   Configuration conf = new Configuration(defaultConf);
    Job job = Job.getInstance(conf);
    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, conf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4comp.blk.gz";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for compressed block");
    	RecordReader<BytesWritable, BytesWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable key = new BytesWritable();
	BytesWritable block = new BytesWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains at least one block");
	block=reader.getCurrentValue();
	assertEquals( 998039, block.getLength(),"Compressed block must have a size of 998.039 bytes");
    	assertFalse( reader.nextKeyValue(),"No further blocks in compressed block");
	reader.close();
  }

  @Test
  public void readBitcoinBlockInputFormatGzipCompressed() throws IOException, InterruptedException{
   Configuration conf = new Configuration(defaultConf);
    Job job = Job.getInstance(conf);
    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, conf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4comp.blk.gz";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for compressed block");
    	RecordReader<BytesWritable, BitcoinBlockWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable key = new BytesWritable();
	BitcoinBlockWritable block = new BitcoinBlockWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains at least one block");
	block=reader.getCurrentValue();
	assertEquals( 936, block.getTransactions().size(),"Compressed block must have at least 936 transactions");
	assertEquals( 4, block.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Compressed block must contain exactly 936 transactions of which the first has one input and script length 4");
	assertEquals( 2, block.getTransactions().get(0).getListOfOutputs().size(),"Compressed block must contain exactly 936 transactions of which the first has two outputs");
	assertEquals( 25, block.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Compressed block must contain exactly 936 transactions of which the first has two output and the first output script length 25");
    	assertFalse( reader.nextKeyValue(),"No further blocks in compressed block");
	reader.close();
  }



  @Test
  public void readBitcoinTransactionInputFormatGzipCompressed() throws IOException, InterruptedException{
 Configuration conf = new Configuration(defaultConf);
    Job job = Job.getInstance(conf);
    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, conf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4comp.blk.gz";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for compressed block");
    	RecordReader<BytesWritable, BitcoinTransactionWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	int transactCount=0;
	while (reader.nextKeyValue()) {
		transactCount++;
	}
 	assertEquals( 936, transactCount,"Comrpessed block must have at least 936 transactions");
	reader.close();
  }



  @Test
  public void readBitcoinRawBlockInputFormatBzip2Compressed() throws IOException, InterruptedException {
 Configuration conf = new Configuration(defaultConf);
    Job job = Job.getInstance(conf);
    CompressionCodec bzip2 = new BZip2Codec();
    ReflectionUtils.setConf(bzip2, conf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4comp.blk.bz2";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinRawBlockFileInputFormat format = new BitcoinRawBlockFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for compressed block");
    	RecordReader<BytesWritable, BytesWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable key = new BytesWritable();
	BytesWritable block = new BytesWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains at least one block");
	block=reader.getCurrentValue();
	assertEquals( 998039, block.getLength(),"Compressed block must have a size of 998.039 bytes");
    	assertFalse( reader.nextKeyValue(),"No further blocks in compressed block");
	reader.close();
  }

  @Test
  public void readBitcoinBlockInputFormatBzip2Compressed() throws IOException, InterruptedException {
 Configuration conf = new Configuration(defaultConf);
    Job job = Job.getInstance(conf);
    CompressionCodec bzip2 = new BZip2Codec();
    ReflectionUtils.setConf(bzip2, conf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4comp.blk.bz2";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinBlockFileInputFormat format = new BitcoinBlockFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for compressed block");
    	RecordReader<BytesWritable, BitcoinBlockWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	BytesWritable key = new BytesWritable();
	BitcoinBlockWritable block = new BitcoinBlockWritable();
	assertTrue( reader.nextKeyValue(),"Input Split for block version contains at least one block");
	block=reader.getCurrentValue();
	assertEquals( 936, block.getTransactions().size(),"Compressed block must have at least 936 transactions");
	assertEquals( 4, block.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Compressed block must contain exactly 936 transactions of which the first has one input and script length 4");
	assertEquals( 2, block.getTransactions().get(0).getListOfOutputs().size(),"Compressed block must contain exactly 936 transactions of which the first has two outputs");
	assertEquals( 25, block.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Compressed block must contain exactly 936 transactions of which the first has two output and the first output script length 25");
    	assertFalse( reader.nextKeyValue(),"No further blocks in compressed block");
	reader.close();
  }


  @Test
  public void readBitcoinTransactionInputFormatBzip2Compressed() throws IOException, InterruptedException {
 Configuration conf = new Configuration(defaultConf);
    Job job = Job.getInstance(conf);
    CompressionCodec bzip2 = new BZip2Codec();
    ReflectionUtils.setConf(bzip2, conf);
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName="version4comp.blk.bz2";
    String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
    Path file = new Path(fileNameBlock);
    FileInputFormat.setInputPaths(job, file);
    BitcoinTransactionFileInputFormat format = new BitcoinTransactionFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    assertEquals( 1, splits.size(),"Only one split generated for compressed block");
    	RecordReader<BytesWritable, BitcoinTransactionWritable> reader = format.createRecordReader(splits.get(0), context);
	assertNotNull( reader,"Format returned  null RecordReader");
	reader.initialize(splits.get(0),context);
	int transactCount=0;
	while (reader.nextKeyValue()) {
		transactCount++;
	}
 	assertEquals( 936, transactCount,"Compressed block must have at least 936 transactions");
	reader.close();
  }




}
