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

package org.zuinnote.hadoop.ethereum.format.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.zuinnote.hadoop.ethereum.format.exception.EthereumBlockReadException;

/**
 * @author jornfranke
 *
 */
public class EthereumFormatReaderTest {
	static final int DEFAULT_BUFFERSIZE=64*1024;
	static final int DEFAULT_MAXSIZE_ETHEREUMBLOCK=1 * 1024 * 1024;
	
	 @Test
	  public void checkTestDataGenesisBlockAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="ethgenesis.bin";
		String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
		assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameGenesis);
		File file = new File(fileNameGenesis);
		assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
		assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
	  }
	 
	 @Test
	  public void parseGenesisBlockAsEthereumRawBlockHeap() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="ethgenesis.bin";
		String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameGenesis);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			ByteBuffer genesisByteBuffer = ebr.readRawBlock();
			assertFalse("Raw Genesis Block is HeapByteBuffer", genesisByteBuffer.isDirect());
			assertEquals("Raw Genesis block has a size of 540 bytes", 540, genesisByteBuffer.limit());
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseGenesisBlockAsEthereumRawBlockDirect() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="ethgenesis.bin";
		String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameGenesis);
		boolean direct=true;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			ByteBuffer genesisByteBuffer = ebr.readRawBlock();
			assertTrue("Raw Genesis Block is DirectByteBuffer", genesisByteBuffer.isDirect());
			assertEquals("Raw Genesis block has a size of 540 bytes", 540, genesisByteBuffer.limit());
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }
	 
	 @Test
	  public void parseGenesisBlockAsEthereumBlockHeap() throws IOException, EthereumBlockReadException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="ethgenesis.bin";
		String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameGenesis);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			EthereumBlock eblock = ebr.readBlock();
			//assertEquals("Raw Genesis block has a size of 540 bytes", 540, genesisByteBuffer.limit());
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
	  }

}
