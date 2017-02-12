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

/**
 * This is an integration test for testing the application with Spark
 */
package org.zuinnote.spark.bitcoin.example;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;


import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.After;


import java.lang.InterruptedException;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.Files;
import java.nio.file.FileVisitResult;
import java.nio.file.SimpleFileVisitor;

import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock;

public class SparkBitcoinBlockCounterSparkMasterIntegrationTest  {
private static String master = "local[2]"; 
private static String appName = "example-sparkbitcoinblockcounter-integrationtest";
private static final String tmpPrefix = "hcl-integrationtest";
private static java.nio.file.Path tmpPath;
private static String CLUSTERNAME="hcl-minicluster";
private static String DFS_INPUT_DIR_NAME = "/input";
private static String DFS_OUTPUT_DIR_NAME = "/output";
private static String DEFAULT_OUTPUT_FILENAME = "part-00000";
private static Path DFS_INPUT_DIR = new Path(DFS_INPUT_DIR_NAME);
private static Path DFS_OUTPUT_DIR = new Path(DFS_OUTPUT_DIR_NAME);
private static int NOOFDATANODES=4;
private static MiniDFSCluster dfsCluster;
private static JavaSparkContext sc;
private static Configuration conf;

private ArrayList<Decompressor> openDecompressors = new ArrayList<>();

   @BeforeClass
    public static void oneTimeSetUp() throws IOException {
     	// Create temporary directory for HDFS base and shutdownhook 
	// create temp directory
      tmpPath = Files.createTempDirectory(tmpPrefix);
      // create shutdown hook to remove temp files (=HDFS MiniCluster) after shutdown, may need to rethink to avoid many threads are created
	Runtime.getRuntime().addShutdownHook(new Thread(
    	new Runnable() {
      	@Override
      	public void run() {
        	try {
          		Files.walkFileTree(tmpPath, new SimpleFileVisitor<java.nio.file.Path>() {
	
            		@Override
            		public FileVisitResult visitFile(java.nio.file.Path file,BasicFileAttributes attrs)
                		throws IOException {
              			Files.delete(file);
             			return FileVisitResult.CONTINUE;
        			}

        		@Override
        		public FileVisitResult postVisitDirectory(java.nio.file.Path dir, IOException e) throws IOException {
          			if (e == null) {
            				Files.delete(dir);
            				return FileVisitResult.CONTINUE;
          			}
          			throw e;
        	}
        	});
      	} catch (IOException e) {
        throw new RuntimeException("Error temporary files in following path could not be deleted "+tmpPath, e);
      }
    }}));
	// Create Configuration
	 conf = new Configuration();
	
	// create HDFS cluster
	File baseDir = new File(tmpPath.toString()).getAbsoluteFile();
	conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
	MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
 	 dfsCluster = builder.numDataNodes(NOOFDATANODES).build();
	conf.set("fs.defaultFS", dfsCluster.getFileSystem().getUri().toString()); 
	// create Spark Context
	SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(master);
      	sc = new JavaSparkContext(sparkConf); 
    }

    @AfterClass
    public static void oneTimeTearDown() throws IOException {
       // destroy Spark cluster
	if (sc!=null) {
		sc.stop();
	}
	// destroy HDFS cluster
	dfsCluster.shutdown();
      }

    @Before
    public void setUp() throws IOException {
	// create input directory
	dfsCluster.getFileSystem().mkdirs(DFS_INPUT_DIR);
    }

    @After
    public void tearDown() throws IOException {
	// Remove input and output directory
	dfsCluster.getFileSystem().delete(DFS_INPUT_DIR,true);
	dfsCluster.getFileSystem().delete(DFS_OUTPUT_DIR,true);
	// close any open decompressor
	for (Decompressor currentDecompressor: this.openDecompressors) {
		if (currentDecompressor!=null) {
			 CodecPool.returnDecompressor(currentDecompressor);
		}
 	}
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
    public void application() throws IOException {
	ClassLoader classLoader = getClass().getClassLoader();
    	// put testdata on DFS
    	String fileName="genesis.blk";
    	String fileNameFullLocal=classLoader.getResource("testdata/"+fileName).getFile();
    	Path inputFile=new Path(fileNameFullLocal);
    	dfsCluster.getFileSystem().copyFromLocalFile(false, false, inputFile, DFS_INPUT_DIR);
	// configure application
	 conf.set("hadoopcryptoledger.bitcoinblockinputformat.filter.magic","F9BEB4D9");
	// submit application to Spark Local
	SparkBitcoinBlockCounter bcc = new SparkBitcoinBlockCounter();
	bcc.jobTotalNumOfTransactions(sc,conf,dfsCluster.getFileSystem().getUri().toString()+DFS_INPUT_DIR_NAME,dfsCluster.getFileSystem().getUri().toString()+DFS_OUTPUT_DIR_NAME);
	// fetch results
	List<String> resultLines = readDefaultResults(1);
    	// compare results
	assertEquals("Number of result line is 1",1,resultLines.size());
	assertEquals("Number of transactions is 1","(No of transactions: ,1)",resultLines.get(0));
    }



      /**
	* Read results from the default output directory and default outputfile name
	*
	* @param numOfRows number of rows to read
	*
	*/
     private List<String> readDefaultResults(int numOfRows) throws IOException {
	ArrayList<String> result = new ArrayList<>();
	Path defaultOutputfile = new Path(DFS_OUTPUT_DIR_NAME+"/"+DEFAULT_OUTPUT_FILENAME);
	InputStream defaultInputStream = openFile(defaultOutputfile);	
	BufferedReader reader=new BufferedReader(new InputStreamReader(defaultInputStream));
	int i=0;
	while(reader.ready())
	{	
		if (i==numOfRows) {
			break;
		}
     		result.add(reader.readLine());
		i++;
	}
	reader.close();
	return result;
	}

/*
* Opens a file using the Hadoop API. It supports uncompressed and compressed files.
*
* @param path path to the file, e.g. file://path/to/file for a local file or hdfs://path/to/file for HDFS file. All filesystem configured for Hadoop can be used
*
* @return InputStream from which the file content can be read
* 
* @throws java.io.Exception in case there is an issue reading the file
*
*
*/

private InputStream openFile(Path path) throws IOException {
        CompressionCodec codec=new CompressionCodecFactory(conf).getCodec(path);
 	FSDataInputStream fileIn=dfsCluster.getFileSystem().open(path);
	// check if compressed
	if (codec==null) { // uncompressed
		return fileIn;
	} else { // compressed
		Decompressor decompressor = CodecPool.getDecompressor(codec);
		this.openDecompressors.add(decompressor); // to be returned later using close
		if (codec instanceof SplittableCompressionCodec) {
			long end = dfsCluster.getFileSystem().getFileStatus(path).getLen(); 
        		final SplitCompressionInputStream cIn =((SplittableCompressionCodec)codec).createInputStream(fileIn, decompressor, 0, end,SplittableCompressionCodec.READ_MODE.CONTINUOUS);
					return cIn;
      		} else {
        		return codec.createInputStream(fileIn,decompressor);
      		}
	}
}

    

}
