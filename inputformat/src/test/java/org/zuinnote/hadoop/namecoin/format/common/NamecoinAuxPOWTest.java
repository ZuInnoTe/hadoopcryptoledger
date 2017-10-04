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
package org.zuinnote.hadoop.namecoin.format.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlockReader;
import org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException;

public class NamecoinAuxPOWTest {
	static final int DEFAULT_BUFFERSIZE=64*1024;
	static final int DEFAULT_MAXSIZE_BITCOINBLOCK=8 * 1024 * 1024;

	static final byte[][] DEFAULT_MAGIC = {{(byte)0xF9,(byte)0xBE,(byte)0xB4,(byte)0xFE}}; // namecoin
	private static final byte[][] TESTNET3_MAGIC = {{(byte)0x0B,(byte)0x11,(byte)0x09,(byte)0x07}};
	private static final byte[][] MULTINET_MAGIC = {{(byte)0xF9,(byte)0xBE,(byte)0xB4,(byte)0xD9},{(byte)0x0B,(byte)0x11,(byte)0x09,(byte)0x07}};

	 @Test
	  public void checkTestDataNamecoinGenesisBlockAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="namecoingenesis.blk";
		String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
		assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameGenesis);
		File file = new File(fileNameGenesis);
		assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
		assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
	  }
	 
	 
	 @Test
	  public void checkTestDataNamecoinRandomBlockAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="namecoinblock.blk";
		String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
		assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameGenesis);
		File file = new File(fileNameGenesis);
		assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
		assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
	  }
	 
	 @Test
	  public void checkTestDataNamecoinThreeDifferentOpinOneBlockAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="namecointhreedifferentopinoneblock.blk";
		String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
		assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameGenesis);
		File file = new File(fileNameGenesis);
		assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
		assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
	  }
	 
	 @Test
	 public void readNoAuxPowNamecoinGenesisBlock() throws BitcoinBlockReadException, IOException {
		 ClassLoader classLoader = getClass().getClassLoader();
			String fileName="namecoingenesis.blk";
			String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
			File file = new File(fullFileNameString);
			BitcoinBlockReader bbr = null;
			boolean direct=false;
			boolean auxPow=true;
			try {
				FileInputStream fin = new FileInputStream(file);
				bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct,auxPow);
				BitcoinBlock theBitcoinBlock = bbr.readBlock();
				assertNotNull("Namecoin Genesis Block contains a block",theBitcoinBlock);
				assertNull("Namecoin Genesis Block has no AuxPowInformation",theBitcoinBlock.getAuxPOW());
				assertEquals("Namecoin Genesis Block must contain exactly one transaction", 1, theBitcoinBlock.getTransactions().size());
				assertEquals("Namecoin Genesis Block must contain exactly one transaction with one input", 1, theBitcoinBlock.getTransactions().get(0).getListOfInputs().size());
				assertEquals("Namecoin Genesis Block must contain exactly one transaction with one input and script length 84", 84, theBitcoinBlock.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length);
				assertEquals("Namecoin Genesis Block must contain exactly one transaction with one output", 1, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().size());
				assertEquals("Namecoin Genesis Block must contain exactly one transaction with one output and script length 67", 67, theBitcoinBlock.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length);
			} finally {
				if (bbr!=null) 
					bbr.close();
			}
	 }
	 
	 @Test
	 public void readAuxPowNamecoinThreeDifferentOpinOneBlock() throws BitcoinBlockReadException, IOException {
		 ClassLoader classLoader = getClass().getClassLoader();
			String fileName="namecointhreedifferentopinoneblock.blk";
			String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
			File file = new File(fullFileNameString);
			BitcoinBlockReader bbr = null;
			boolean direct=false;
			boolean auxPow=true;
			try {
				FileInputStream fin = new FileInputStream(file);
				bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct,auxPow);
				BitcoinBlock theBitcoinBlock = bbr.readBlock();
				assertNotNull("Namecoin Three Different Op in One Block contains a block",theBitcoinBlock);
				assertNotNull("Namecoin Three Different Op in Block has AuxPowInformation",theBitcoinBlock.getAuxPOW());
				assertEquals("Namecoin Three Different Op in Block must contain exactly 7 transactions", 7, theBitcoinBlock.getTransactions().size());
					} finally {
				if (bbr!=null) 
					bbr.close();
			}
	 }
	 
	 
}
