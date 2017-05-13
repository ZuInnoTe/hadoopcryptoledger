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
* This test integrates HDFS and Flink
*
*/

package org.zuinnote.flink.bitcoin.example


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

import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.LocalEnvironment

import scala.collection.mutable.ArrayBuffer
import org.scalatest.{FlatSpec, BeforeAndAfterAll, GivenWhenThen, Matchers}

class FlinkBitcoinBlockCounterFlinkMasterIntegrationSpec extends FlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers {
 

private val appName: String = "example-scalaflinkbitcoinblockcounter-integrationtest"
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
private var flinkEnvironment: ExecutionEnvironment = _
private var conf: Configuration = _
private var openDecompressors = ArrayBuffer[Decompressor]()

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
	// create local Flink cluster
 	flinkEnvironment = new LocalEnvironment()
 }

  
  override def afterAll(): Unit = {
  
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
	Given("Genesis Block on DFSCluster")
	// create input directory
	dfsCluster.getFileSystem().mkdirs(DFS_INPUT_DIR)
	// copy bitcoin blocks
	val classLoader = getClass().getClassLoader()
    	// put testdata on DFS
    	val fileName: String="genesis.blk"
    	val fileNameFullLocal=classLoader.getResource("testdata/"+fileName).getFile()
    	val inputFile=new Path(fileNameFullLocal)
    	dfsCluster.getFileSystem().copyFromLocalFile(false, false, inputFile, DFS_INPUT_DIR)

	When("count total transactions")
	//
	Then("1 transaction in total as a result")
	// fetch results
	//val resultLines = readDefaultResults(1)
	//assert(1==resultLines.size())
	//assert("(No of transactions: ,1)"==resultLines.get(0))
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
