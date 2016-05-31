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

package org.zuinnote.hadoop.bitcoin.hive.serde;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.zuinnote.hadoop.bitcoin.format.BitcoinBlock;
import org.zuinnote.hadoop.bitcoin.format.BitcoinBlockReader;
import org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException;


import org.zuinnote.hadoop.bitcoin.hive.serde.struct.BitcoinBlockStruct;

public class BitcoinHiveSerdeTest {
private static final int DEFAULT_BUFFERSIZE=64*1024;
private static final int DEFAULT_MAXSIZE_BITCOINBLOCK=1 * 1024 * 1024;
private static final byte[][] DEFAULT_MAGIC = {{(byte)0xF9,(byte)0xBE,(byte)0xB4,(byte)0xD9}};

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
  public void deserialize() throws  FileNotFoundException, IOException, BitcoinBlockReadException {
	BitcoinBlockSerde testSerde = new BitcoinBlockSerde();
	// create a BitcoinBlock based on the genesis block test data
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="genesis.blk";
	String fullFileNameString=classLoader.getResource("testdata/"+fileName).getFile();	
	File file = new File(fullFileNameString);
	BitcoinBlockReader bbr = null;
	boolean direct=false;
	try {
		FileInputStream fin = new FileInputStream(file);
		bbr = new BitcoinBlockReader(fin,this.DEFAULT_MAXSIZE_BITCOINBLOCK,this.DEFAULT_BUFFERSIZE,this.DEFAULT_MAGIC,direct);
		BitcoinBlock theBitcoinBlock = bbr.readBlock();
	// deserialize it
		Object deserializedObject = testSerde.deserialize(theBitcoinBlock);
		assertTrue("Deserialized Object is of type BitcoinBlockStruct", deserializedObject instanceof BitcoinBlockStruct);
		BitcoinBlockStruct deserializedBitcoinBlockStruct = (BitcoinBlockStruct)deserializedObject;
	// verify certain attributes
		assertEquals("Genesis Block must contain exactly one transaction", 1, deserializedBitcoinBlockStruct.transactions.size());
		assertEquals("Genesis Block must contain exactly one transaction with one input", 1, deserializedBitcoinBlockStruct.transactions.get(0).listOfInputs.size());
		assertEquals("Genesis Block must contain exactly one transaction with one input and script length 77", 77, deserializedBitcoinBlockStruct.transactions.get(0).listOfInputs.get(0).txInScript.length);
		assertEquals("Genesis Block must contain exactly one transaction with one output", 1, deserializedBitcoinBlockStruct.transactions.get(0).listOfOutputs.size());
		assertEquals("Genesis Block must contain exactly one transaction with one output and script length 67", 67, deserializedBitcoinBlockStruct.transactions.get(0).listOfOutputs.get(0).txOutScript.length);
	} finally {
		if (bbr!=null) 
			bbr.close();
	}
  }


} 


