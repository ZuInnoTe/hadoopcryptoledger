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


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlockWritable;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlockReader;
import org.zuinnote.hadoop.bitcoin.format.exception.BitcoinBlockReadException;
import org.zuinnote.hadoop.bitcoin.format.mapred.AbstractBitcoinRecordReader;
import org.zuinnote.hadoop.bitcoin.hive.datatypes.HiveBitcoinBlock;



public class BitcoinHiveSerdeTest {
private static final int DEFAULT_BUFFERSIZE=AbstractBitcoinRecordReader.DEFAULT_BUFFERSIZE;
private static final int DEFAULT_MAXSIZE_BITCOINBLOCK=AbstractBitcoinRecordReader.DEFAULT_MAXSIZE_BITCOINBLOCK;
private static final byte[][] DEFAULT_MAGIC = {{(byte)0xF9,(byte)0xBE,(byte)0xB4,(byte)0xD9}};

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
  public void initializePositive() {
	BitcoinBlockSerde testSerde = new BitcoinBlockSerde();
	Configuration conf = new Configuration();
	Properties tblProperties = new Properties();
	// just for testing purposes - these values may have no real meaning
	tblProperties.setProperty(BitcoinBlockSerde.CONF_MAXBLOCKSIZE, String.valueOf(1));
        tblProperties.setProperty(BitcoinBlockSerde.CONF_FILTERMAGIC, "A0A0A0A0");
	tblProperties.setProperty(BitcoinBlockSerde.CONF_USEDIRECTBUFFER,"true");
	tblProperties.setProperty(BitcoinBlockSerde.CONF_ISSPLITABLE,"true");
	tblProperties.setProperty(BitcoinBlockSerde.CONF_READAUXPOW,"true");
	testSerde.initialize(conf,tblProperties);
	assertEquals(1, conf.getInt(BitcoinBlockSerde.CONF_MAXBLOCKSIZE,2),"MAXBLOCKSIZE set correctly");
	assertEquals("A0A0A0A0", conf.get(BitcoinBlockSerde.CONF_FILTERMAGIC,"B0B0B0B0"),"FILTERMAGIC set correctly");
	assertTrue(conf.getBoolean(BitcoinBlockSerde.CONF_USEDIRECTBUFFER,false),"USEDIRECTBUFFER set correctly");
	assertTrue(conf.getBoolean(BitcoinBlockSerde.CONF_ISSPLITABLE,false),"ISSPLITABLE set correctly");
	assertTrue(conf.getBoolean(BitcoinBlockSerde.CONF_READAUXPOW,false),"ISSPLITABLE set correctly");
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
		BitcoinBlock readBitcoinBlock = bbr.readBlock();
    BitcoinBlockWritable theBitcoinBlock = new BitcoinBlockWritable();
    theBitcoinBlock.set(readBitcoinBlock);
	// deserialize it
		Object deserializedObject = testSerde.deserialize(theBitcoinBlock);
		assertTrue( deserializedObject instanceof HiveBitcoinBlock,"Deserialized Object is of type BitcoinBlock");
		HiveBitcoinBlock deserializedBitcoinBlockStruct = (HiveBitcoinBlock)deserializedObject;
	// verify certain attributes
		assertEquals( 1, deserializedBitcoinBlockStruct.getTransactions().size(),"Genesis Block must contain exactly one transaction");
		assertEquals( 1, deserializedBitcoinBlockStruct.getTransactions().get(0).getListOfInputs().size(),"Genesis Block must contain exactly one transaction with one input");
		assertEquals( 77, deserializedBitcoinBlockStruct.getTransactions().get(0).getListOfInputs().get(0).getTxInScript().length,"Genesis Block must contain exactly one transaction with one input and script length 77");
		assertEquals( 0, HiveDecimal.create(5000000000L).compareTo(deserializedBitcoinBlockStruct.getTransactions().get(0).getListOfOutputs().get(0).getValue()), "Value must be BigInteger corresponding to 5000000000L");


		assertEquals( 1, deserializedBitcoinBlockStruct.getTransactions().get(0).getListOfOutputs().size(),"Genesis Block must contain exactly one transaction with one output");
		assertEquals( 67, deserializedBitcoinBlockStruct.getTransactions().get(0).getListOfOutputs().get(0).getTxOutScript().length,"Genesis Block must contain exactly one transaction with one output and script length 67");
	} finally {
		if (bbr!=null)
			bbr.close();
	}
  }


}
