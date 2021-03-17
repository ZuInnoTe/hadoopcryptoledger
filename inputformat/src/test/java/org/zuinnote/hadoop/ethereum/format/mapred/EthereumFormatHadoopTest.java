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
import java.util.List;

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

import org.zuinnote.hadoop.ethereum.format.common.EthereumBlockWritable;
import org.zuinnote.hadoop.ethereum.format.common.EthereumBlockHeader;
import org.zuinnote.hadoop.ethereum.format.exception.EthereumBlockReadException;

/**
 * @author jornfranke
 *
 */
public class EthereumFormatHadoopTest {
	private static Configuration defaultConf = new Configuration();
	private static FileSystem localFs = null;
	private static Reporter reporter = Reporter.NULL;

	private final static char[] hexArray = "0123456789ABCDEF".toCharArray();
	private static String bytesToHex(byte[] bytes) {
		char[] hexChars = new char[bytes.length * 2];
		for ( int j = 0; j < bytes.length; j++ ) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}
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
			public void checkTestDataBlock403419() {
				ClassLoader classLoader = getClass().getClassLoader();
				String fileName="block403419.bin";
				String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();
				assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
				File file = new File(fileNameGenesis);
				assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
				assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
			}
			@Test
			public void checkTestDataBlock447533() {
				ClassLoader classLoader = getClass().getClassLoader();
				String fileName="block447533.bin";
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
		    	RecordReader<BytesWritable, EthereumBlockWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
			assertNotNull( reader,"Format returned  null RecordReader");
			BytesWritable key = new BytesWritable();
			EthereumBlockWritable  block = new EthereumBlockWritable();
			assertTrue( reader.next(key,block),"Input Split for genesis block contains at least one block");
			assertEquals( 0, block.getEthereumTransactions().size(),"Genesis Block must have 0 transactions");
		    	assertFalse( reader.next(key,block),"No further blocks in genesis Block");
		    	reader.close();
			}


			@Test
			public void readEthereumBlockInputFormatBlock403419() throws IOException, EthereumBlockReadException, ParseException, InterruptedException {
				JobConf job = new JobConf(defaultConf);
				ClassLoader classLoader = getClass().getClassLoader();
				String fileName="block403419.bin";
				String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
				Path file = new Path(fileNameBlock);
			    FileInputFormat.setInputPaths(job, file);
			    EthereumBlockFileInputFormat format = new EthereumBlockFileInputFormat();
			    format.configure(job);
			    InputSplit[] inputSplits = format.getSplits(job,1);

			    assertEquals( 1, inputSplits.length,"Only one split generated for block 403419");
			    	RecordReader<BytesWritable, EthereumBlockWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
				assertNotNull( reader,"Format returned  null RecordReader");
				BytesWritable key = new BytesWritable();
				EthereumBlockWritable  block = new EthereumBlockWritable();
				assertTrue( reader.next(key,block),"Input Split for block 403419 contains at least one block");
				assertEquals( 2, block.getEthereumTransactions().size(),"Block 403419 must have 2 transactions");
				EthereumBlockHeader ethereumBlockHeader = block.getEthereumBlockHeader();
				assertEquals(
						"f8b483dba2c3b7176a3da549ad41a48bb3121069",
						bytesToHex(ethereumBlockHeader.getCoinBase()).toLowerCase(),
						"Block 403419 was mined by f8b483dba2c3b7176a3da549ad41a48bb3121069"
				);
				assertEquals(
						"08741fa532c05804d9c1086a311e47cc024bbc43980f561041ad1fbb3c223322",
						bytesToHex(ethereumBlockHeader.getParentHash()).toLowerCase(),
						"The parent of block 403419 has hash 08741fa532c05804d9c1086a311e47cc024bbc43980f561041ad1fbb3c223322"
				);
			    	assertFalse( reader.next(key,block),"No further lock 403419  in genesis Block");

			    	reader.close();

			}

			@Test
			public void readEthereumBlockInputFormatBlock447533() throws IOException, EthereumBlockReadException, ParseException, InterruptedException {
				JobConf job = new JobConf(defaultConf);
				ClassLoader classLoader = getClass().getClassLoader();
				String fileName="block447533.bin";
				String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();
				Path file = new Path(fileNameBlock);
			    FileInputFormat.setInputPaths(job, file);
			    EthereumBlockFileInputFormat format = new EthereumBlockFileInputFormat();
			    format.configure(job);
			    InputSplit[] inputSplits = format.getSplits(job,1);

			    assertEquals( 1, inputSplits.length,"Only one split generated for block 447533");
			    	RecordReader<BytesWritable, EthereumBlockWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
				assertNotNull( reader,"Format returned  null RecordReader");
				BytesWritable key = new BytesWritable();
				EthereumBlockWritable  block = new EthereumBlockWritable();
				assertTrue( reader.next(key,block),"Input Split for block 447533 contains at least one block");
				assertEquals( 2, block.getEthereumTransactions().size(),"Block 447533 must have 2 transactions");
				EthereumBlockHeader ethereumBlockHeader = block.getEthereumBlockHeader();
				assertEquals(
						"a027231f42c80ca4125b5cb962a21cd4f812e88f",
						bytesToHex(ethereumBlockHeader.getCoinBase()).toLowerCase(),
						"Block 447533 was mined by a027231f42c80ca4125b5cb962a21cd4f812e88f"
				);
				assertEquals(
						"043559b70c54f0eea6a90b384286d7ab312129603e750075d09fd35e66f8068a",
						bytesToHex(ethereumBlockHeader.getParentHash()).toLowerCase(),
						"The parent of block 447533 has hash 043559b70c54f0eea6a90b384286d7ab312129603e750075d09fd35e66f8068a"
				);
			    	assertFalse( reader.next(key,block),"No further block  in  block 447533");

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
		    	RecordReader<BytesWritable, EthereumBlockWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
			assertNotNull( reader,"Format returned  null RecordReader");
			BytesWritable key = new BytesWritable();
			EthereumBlockWritable  block = new EthereumBlockWritable();
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
		    	RecordReader<BytesWritable, EthereumBlockWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
			assertNotNull( reader,"Format returned  null RecordReader");

			BytesWritable key = new BytesWritable();
			EthereumBlockWritable  block = new EthereumBlockWritable();
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
		    	RecordReader<BytesWritable, EthereumBlockWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
			assertNotNull( reader,"Format returned  null RecordReader");

			BytesWritable key = new BytesWritable();
			EthereumBlockWritable  block = new EthereumBlockWritable();
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
		    	RecordReader<BytesWritable, EthereumBlockWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
			assertNotNull( reader,"Format returned  null RecordReader");
			BytesWritable key = new BytesWritable();
			EthereumBlockWritable  block = new EthereumBlockWritable();
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
		    	RecordReader<BytesWritable, EthereumBlockWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
			assertNotNull( reader,"Format returned  null RecordReader");
			BytesWritable key = new BytesWritable();
			EthereumBlockWritable  block = new EthereumBlockWritable();
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
		    	RecordReader<BytesWritable, EthereumBlockWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
			assertNotNull( reader,"Format returned  null RecordReader");
			BytesWritable key = new BytesWritable();
			EthereumBlockWritable  block = new EthereumBlockWritable();
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
		    	RecordReader<BytesWritable, EthereumBlockWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
			assertNotNull( reader,"Format returned  null RecordReader");

			BytesWritable key = new BytesWritable();
			EthereumBlockWritable  block = new EthereumBlockWritable();
			assertTrue( reader.next(key,block),"Input Split for block 1346406 contains at least one block");

			assertEquals( 6, block.getEthereumTransactions().size(),"Block 1346406 must have 6 transactions");
		    	assertFalse( reader.next(key,block),"No further blocks in block 1346406");
		    	reader.close();
			}
}
