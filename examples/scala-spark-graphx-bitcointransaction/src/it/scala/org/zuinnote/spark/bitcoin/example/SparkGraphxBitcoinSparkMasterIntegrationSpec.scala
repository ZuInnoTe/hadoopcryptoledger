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

/**
*
* This test intregrates HDFS and Spark
*
*/

package org.zuinnote.spark.bitcoin.example


import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.Path

import java.io.BufferedReader
import java.io.File
import java.io.InputStream
import java.io.InputStreamReader
import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.Files
import java.nio.file.FileVisitResult
import java.nio.file.SimpleFileVisitor
import java.util.ArrayList
import java.util.List


import org.apache.hadoop.io.compress.CodecPool
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.compress.Decompressor
import org.apache.hadoop.io.compress.SplittableCompressionCodec
import org.apache.hadoop.io.compress.SplitCompressionInputStream


import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import org.scalatest.flatspec.AnyFlatSpec;
import org.scalatest._
import matchers.should._
import org.scalatest.{ BeforeAndAfterAll, GivenWhenThen }

class SparkGraphxBitcoinSparkMasterIntegrationSpec extends AnyFlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers {
 
private var sc: SparkContext = _
private val master: String = "local[2]"
private val appName: String = "example-scalasparkgraphxbitcoin-integrationtest"
private val tmpPrefix: String = "hcl-integrationtest"
private var tmpPath: java.nio.file.Path = _
private val CLUSTERNAME: String ="hcl-minicluster"
private val DFS_INPUT_DIR_NAME: String = "/input"
private val DFS_OUTPUT_DIR_NAME: String = "/output"
private val DEFAULT_OUTPUT_FILENAME: String = "part-00000"
private val DFS_INPUT_DIR : Path = new Path(DFS_INPUT_DIR_NAME)
private val DFS_OUTPUT_DIR : Path = new Path(DFS_OUTPUT_DIR_NAME)
private val NOOFDATANODES: Int =4
private var dfsCluster: MiniDFSCluster = _
private var conf: Configuration = _
private var openDecompressors = ArrayBuffer[Decompressor]();

override def beforeAll(): Unit = {
    super.beforeAll()

		// Create temporary directory for HDFS base and shutdownhook 
	// create temp directory
      tmpPath = Files.createTempDirectory(tmpPrefix)
      // create shutdown hook to remove temp files (=HDFS MiniCluster) after shutdown, may need to rethink to avoid many threads are created
	Runtime.getRuntime.addShutdownHook(new Thread("remove temporary directory") {
      	 override def run(): Unit =  {
        	try {
          		Files.walkFileTree(tmpPath, new SimpleFileVisitor[java.nio.file.Path]() {

            		override def visitFile(file: java.nio.file.Path,attrs: BasicFileAttributes): FileVisitResult = {
                		Files.delete(file)
             			return FileVisitResult.CONTINUE
        			}

        		override def postVisitDirectory(dir: java.nio.file.Path, e: IOException): FileVisitResult = {
          			if (e == null) {
            				Files.delete(dir)
            				return FileVisitResult.CONTINUE
          			}
          			throw e
        			}
        	})
      	} catch {
        case e: IOException => throw new RuntimeException("Error temporary files in following path could not be deleted "+tmpPath, e)
    }}})
	// create DFS mini cluster
	 conf = new Configuration()
	val baseDir = new File(tmpPath.toString()).getAbsoluteFile()
	conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath())
	val builder = new MiniDFSCluster.Builder(conf)
 	 dfsCluster = builder.numDataNodes(NOOFDATANODES).build()
	conf.set("fs.defaultFS", dfsCluster.getFileSystem().getUri().toString()) 
	// create local Spark cluster
 	val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
	  .set( "spark.driver.host", "localhost" )
	sc = new SparkContext(sparkConf)
 }

  
  override def afterAll(): Unit = {
   // close Spark Context
    if (sc!=null) {
	sc.stop()
    } 
    // close decompressor
	for ( currentDecompressor <- this.openDecompressors) {
		if (currentDecompressor!=null) {
			 CodecPool.returnDecompressor(currentDecompressor)
		}
 	}
    // close dfs cluster
    dfsCluster.shutdown()
    super.afterAll()
}


"The genesis block on DFS" should "be 1 transactions i total" in {
	Given("First blocks on DFSCluster")
	// create input directory
	dfsCluster.getFileSystem().mkdirs(DFS_INPUT_DIR)
	// copy bitcoin blocks
	val classLoader = getClass().getClassLoader()
    	// put testdata on DFS
    	val fileName: String="firstblocks.blk"
    	val fileNameFullLocal=classLoader.getResource("testdata/"+fileName).getFile()
    	val inputFile=new Path(fileNameFullLocal)
    	dfsCluster.getFileSystem().copyFromLocalFile(false, false, inputFile, DFS_INPUT_DIR)	
	Given("Configuration")
	 conf.set("hadoopcryptoledger.bitcoinblockinputformat.filter.magic","F9BEB4D9")
	When("count total transactions")
	SparkScalaBitcoinTransactionGraph.jobTop5AddressInput(sc,conf,dfsCluster.getFileSystem().getUri().toString()+DFS_INPUT_DIR_NAME,dfsCluster.getFileSystem().getUri().toString()+DFS_OUTPUT_DIR_NAME)
	Then("5 transaction in total as a result")
	// fetch results
	val resultLines = readDefaultResults(5)
	assert(5==resultLines.size())
	assert(resultLines.get(0).contains("(bitcoinpubkey_41046A0765B5865641CE08DD39690AADE26DFBF5511430CA428A3089261361CEF170E3929A68AEE3D8D4848B0C5111B0A37B82B86AD559FD2A745B44D8E8D9DFDC0C,12)"))
	assert(resultLines.get(1).contains("(bitcoinpubkey_410411DB93E1DCDB8A016B49840F8C53BC1EB68A382E97B1482ECAD7B148A6909A5CB2E0EADDFB84CCF9744464F82E160BFA9B8B64F9D4C03F999B8643F656B412A3,10)"))
	assert(resultLines.get(2).contains("(bitcoinpubkey_41044A656F065871A353F216CA26CEF8DDE2F03E8C16202D2E8AD769F02032CB86A5EB5E56842E92E19141D60A01928F8DD2C875A390F67C1F6C94CFC617C0EA45AF,6)"))
	assert(resultLines.get(3).contains("(bitcoinpubkey_4104F36C67039006EC4ED2C885D7AB0763FEB5DEB9633CF63841474712E4CF0459356750185FC9D962D0F4A1E08E1A84F0C9A9F826AD067675403C19D752530492DC,4)"))
	assert(resultLines.get(4).contains("(bitcoinpubkey_41048B48E109F432A490522F8D0E9E833443809F65B8AA2558B94C1C15EB0FD3E24F32D58A088A2B0E5E694D05E7B981E5C9750827646EB81DEBC816C3C667EEA5FD,2)"))
}




      /**
	* Read results from the default output directory and default outputfile name
	*
	* @param numOfRows number of rows to read
	*
	*/
     def readDefaultResults(numOfRows: Int): List[String] = {
	val result: ArrayList[String] = new ArrayList[String]()
	val defaultOutputfile = new Path(DFS_OUTPUT_DIR_NAME+"/"+DEFAULT_OUTPUT_FILENAME)
	val defaultInputStream = openFile(defaultOutputfile)
	val reader=new BufferedReader(new InputStreamReader(defaultInputStream))
	var i=0
	while((reader.ready()) && (i!=numOfRows))
	{	
     		result.add(reader.readLine())
		i += 1
	}
	reader.close()
	return result
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

def  openFile(path: Path): InputStream = {
        val codec=new CompressionCodecFactory(conf).getCodec(path)
 	val fileIn: InputStream=dfsCluster.getFileSystem().open(path)
	// check if compressed
	if (codec==null) { // uncompressed
		return fileIn
	} else { // compressed
		val decompressor: Decompressor = CodecPool.getDecompressor(codec)
		openDecompressors+=decompressor // to be returned later using close
		if (codec.isInstanceOf[SplittableCompressionCodec]) {
			val end : Long = dfsCluster.getFileSystem().getFileStatus(path).getLen() 
        		val  cIn =codec.asInstanceOf[SplittableCompressionCodec].createInputStream(fileIn, decompressor, 0, end,SplittableCompressionCodec.READ_MODE.CONTINUOUS)
					return cIn
      		} else {
        		return codec.createInputStream(fileIn,decompressor)
      		}
	}
}

}
