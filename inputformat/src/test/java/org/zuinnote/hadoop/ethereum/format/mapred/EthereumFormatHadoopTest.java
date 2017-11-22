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

package org.zuinnote.hadoop.ethereum.format.mapred;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.ethereum.format.common.EthereumBlock;
import org.zuinnote.hadoop.ethereum.format.exception.EthereumBlockReadException;

/**
 * @author jornfranke
 *
 */
public class EthereumFormatHadoopTest {
	private static Configuration defaultConf = new Configuration();
	private static FileSystem localFs = null; 
	private static Reporter reporter = Reporter.NULL;
	
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
		  public void checkTestDataBlock1346406Bzip2CompressedAvailable() {
			ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth1346406.bin.bz2";
			String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
			assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
			File file = new File(fileNameGenesis);
			assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
			assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
		  }
		 
		 @Test
		  public void checkTestDataBlock1346406AGzipCompressedvailable() {
			ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth1346406.bin.gz";
			String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
			assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
			File file = new File(fileNameGenesis);
			assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
			assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
		  }
		 
		 @Test
		  public void readEthereumBlockInputFormatGenesisBlock() throws IOException, EthereumBlockReadException, ParseException, InterruptedException {
			JobConf job = new JobConf(defaultConf);
			ClassLoader classLoader = getClass().getClassLoader();
			String fileName="ethgenesis.bin";
			String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
			Path file = new Path(fileNameBlock);
		    FileInputFormat.setInputPaths(job, file);
		    EthereumBlockFileInputFormat format = new EthereumBlockFileInputFormat();
		    format.configure(job);
		    InputSplit[] inputSplits = format.getSplits(job,1);
		  
		    assertEquals( 1, inputSplits.length,"Only one split generated for genesis block");
		    	RecordReader<BytesWritable, EthereumBlock> reader = format.getRecordReader(inputSplits[0], job, reporter);
			assertNotNull( reader,"Format returned  null RecordReader");
			BytesWritable key = new BytesWritable();	
			EthereumBlock block = new EthereumBlock();
			assertTrue( reader.next(key,block),"Input Split for genesis block contains at least one block");
			assertEquals( 0, block.getEthereumTransactions().size(),"Genesis Block must have 0 transactions");
		    	assertFalse( reader.next(key,block),"No further blocks in genesis Block");
		    	reader.close();
			}
		 
		 @Test
		  public void readEthereumBlockInputFormatBlock1() throws IOException, EthereumBlockReadException, ParseException, InterruptedException {
				JobConf job = new JobConf(defaultConf);
						ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth1.bin";
			String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
			Path file = new Path(fileNameBlock);
		    FileInputFormat.setInputPaths(job, file);
		    EthereumBlockFileInputFormat format = new EthereumBlockFileInputFormat();
		    format.configure(job);
		    InputSplit[] inputSplits = format.getSplits(job,1);
		  
		    assertEquals( 1, inputSplits.length,"Only one split generated for genesis block");
		    	RecordReader<BytesWritable, EthereumBlock> reader = format.getRecordReader(inputSplits[0], job, reporter);
			assertNotNull( reader,"Format returned  null RecordReader");
			BytesWritable key = new BytesWritable();	
			EthereumBlock block = new EthereumBlock();
			assertTrue( reader.next(key,block),"Input Split for block 1 contains at least one block");
			assertEquals( 0, block.getEthereumTransactions().size(),"Block 1 must have 0 transactions");
		    	assertFalse( reader.next(key,block),"No further blocks in block 1");
		    	reader.close();
			}
		 
		 
		 @Test
		  public void readEthereumBlockInputFormatBlock1346406() throws IOException, EthereumBlockReadException, ParseException, InterruptedException {
				JobConf job = new JobConf(defaultConf);
							ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth1346406.bin";
			String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
			Path file = new Path(fileNameBlock);
		    FileInputFormat.setInputPaths(job, file);
		    EthereumBlockFileInputFormat format = new EthereumBlockFileInputFormat();
		    format.configure(job);
		    InputSplit[] inputSplits = format.getSplits(job,1);
		  
		    assertEquals( 1, inputSplits.length,"Only one split generated for genesis block");
		    	RecordReader<BytesWritable, EthereumBlock> reader = format.getRecordReader(inputSplits[0], job, reporter);
			assertNotNull( reader,"Format returned  null RecordReader");
		
			BytesWritable key = new BytesWritable();	
			EthereumBlock block = new EthereumBlock();
			assertTrue( reader.next(key,block),"Input Split for block 1346406 contains at least one block");
			assertEquals( 6, block.getEthereumTransactions().size(),"Block 1346406 must have 6 transactions");
		    	assertFalse( reader.next(key,block),"No further blocks in block 1346406");
		    	reader.close();
			}
		 
		 @Test
		  public void readEthereumBlockInputFormatBlock3346406() throws IOException, EthereumBlockReadException, ParseException, InterruptedException {
			JobConf job = new JobConf(defaultConf);
			ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth3346406.bin";
			String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
			Path file = new Path(fileNameBlock);
		    FileInputFormat.setInputPaths(job, file);
		    EthereumBlockFileInputFormat format = new EthereumBlockFileInputFormat();
		    format.configure(job);
		    InputSplit[] inputSplits = format.getSplits(job,1);
		  
		    assertEquals( 1, inputSplits.length,"Only one split generated for genesis block");
		    	RecordReader<BytesWritable, EthereumBlock> reader = format.getRecordReader(inputSplits[0], job, reporter);
			assertNotNull( reader,"Format returned  null RecordReader");

			BytesWritable key = new BytesWritable();	
			EthereumBlock block = new EthereumBlock();
			assertTrue( reader.next(key,block),"Input Split for block 3346406 contains at least one block");
			assertEquals( 7, block.getEthereumTransactions().size(),"Block 3346406 must have 7 transactions");
		    	assertFalse( reader.next(key,block),"No further blocks in block 3346406");
		    	reader.close();
			}
		 
		 
		 @Test
		  public void readEthereumBlockInputFormatBlock0to10() throws IOException, EthereumBlockReadException, ParseException, InterruptedException {
			JobConf job = new JobConf(defaultConf);
			ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth0to10.bin";
			String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
			Path file = new Path(fileNameBlock);
		    FileInputFormat.setInputPaths(job, file);
		    EthereumBlockFileInputFormat format = new EthereumBlockFileInputFormat();
		    format.configure(job);
		    InputSplit[] inputSplits = format.getSplits(job,1);
		  
		    assertEquals( 1, inputSplits.length,"Only one split generated for genesis block");
		    	RecordReader<BytesWritable, EthereumBlock> reader = format.getRecordReader(inputSplits[0], job, reporter);
			assertNotNull( reader,"Format returned  null RecordReader");
			BytesWritable key = new BytesWritable();	
			EthereumBlock block = new EthereumBlock();
			int count=0;
			while (count<11) {
				if (reader.next(key,block)) {
					count++;
				}
			}
			assertEquals(11,count,"Block 0..10 contains 11 blocks");

		    	assertFalse( reader.next(key,block),"No further blocks in block 0..10");
		    	reader.close();
			}
		 
		 @Test
		  public void readEthereumBlockInputFormatBlock3510000to3510010() throws IOException, EthereumBlockReadException, ParseException, InterruptedException {
				JobConf job = new JobConf(defaultConf);
							ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth351000to3510010.bin";
			String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
			Path file = new Path(fileNameBlock);
		    FileInputFormat.setInputPaths(job, file);
		    EthereumBlockFileInputFormat format = new EthereumBlockFileInputFormat();
		    format.configure(job);
		    InputSplit[] inputSplits = format.getSplits(job,1);
		  
		    assertEquals( 1, inputSplits.length,"Only one split generated for genesis block");
		    	RecordReader<BytesWritable, EthereumBlock> reader = format.getRecordReader(inputSplits[0], job, reporter);
			assertNotNull( reader,"Format returned  null RecordReader");
			BytesWritable key = new BytesWritable();	
			EthereumBlock block = new EthereumBlock();
			int count=0;
			while (count<11) {
				if (reader.next(key,block)) {
					count++;
				}
			}
			assertEquals(11,count,"Block 3510000 .. 3510010 contains 11 blocks");

		    	assertFalse( reader.next(key,block),"No further blocks in block 3510000 .. 3510010");
		    	reader.close();
			}
		 
		 @Test
		  public void readEthereumBlockInputFormatBlock1346406GzipCompressed() throws IOException, EthereumBlockReadException, ParseException, InterruptedException {
				JobConf job = new JobConf(defaultConf);
							ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth1346406.bin.gz";
			String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
			Path file = new Path(fileNameBlock);
		    FileInputFormat.setInputPaths(job, file);
		    EthereumBlockFileInputFormat format = new EthereumBlockFileInputFormat();
		    format.configure(job);
		    InputSplit[] inputSplits = format.getSplits(job,1);
		  
		    assertEquals( 1, inputSplits.length,"Only one split generated for genesis block");
		    	RecordReader<BytesWritable, EthereumBlock> reader = format.getRecordReader(inputSplits[0], job, reporter);
			assertNotNull( reader,"Format returned  null RecordReader");
			BytesWritable key = new BytesWritable();	
			EthereumBlock block = new EthereumBlock();
			assertTrue( reader.next(key,block),"Input Split for block 1346406 contains at least one block");
			assertEquals( 6, block.getEthereumTransactions().size(),"Block 1346406 must have 6 transactions");
		    	assertFalse( reader.next(key,block),"No further blocks in block 1346406");
		    	reader.close();
			}
		 
		 @Test
		  public void readEthereumBlockInputFormatBlock1346406Bzip2Compressed() throws IOException, EthereumBlockReadException, ParseException, InterruptedException {
				JobConf job = new JobConf(defaultConf);
			ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth1346406.bin.bz2";
			String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
			Path file = new Path(fileNameBlock);
		    FileInputFormat.setInputPaths(job, file);
		    EthereumBlockFileInputFormat format = new EthereumBlockFileInputFormat();
		    format.configure(job);
		    InputSplit[] inputSplits = format.getSplits(job,1);
		  
		    assertEquals( 1, inputSplits.length,"Only one split generated for genesis block");
		    	RecordReader<BytesWritable, EthereumBlock> reader = format.getRecordReader(inputSplits[0], job, reporter);
			assertNotNull( reader,"Format returned  null RecordReader");
	
			BytesWritable key = new BytesWritable();	
			EthereumBlock block = new EthereumBlock();
			assertTrue( reader.next(key,block),"Input Split for block 1346406 contains at least one block");
	
			assertEquals( 6, block.getEthereumTransactions().size(),"Block 1346406 must have 6 transactions");
		    	assertFalse( reader.next(key,block),"No further blocks in block 1346406");
		    	reader.close();
			}
}
