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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Test;
import org.zuinnote.hadoop.ethereum.format.common.EthereumUtil;
import org.zuinnote.hadoop.ethereum.format.common.rlp.RLPElement;
import org.zuinnote.hadoop.ethereum.format.common.rlp.RLPList;
import org.zuinnote.hadoop.ethereum.format.common.rlp.RLPObject;
import org.zuinnote.hadoop.ethereum.format.exception.EthereumBlockReadException;

public class EthereumUtilTest {
	static final int DEFAULT_BUFFERSIZE=64*1024;
	static final int DEFAULT_MAXSIZE_ETHEREUMBLOCK=1 * 1024 * 1024;
	
	public final static byte[] TEST_RLP_ELEMENT_STRING = new byte[] {(byte) 0x83,'d','o','g'};
	public final static byte[] TEST_RLP_LIST_STRING = new byte[] {(byte) 0xc8,(byte) 0x83,'c','a','t',(byte) 0x83,'d','o','g'};
	public final static byte[] TEST_RLP_EMPTY_STRING = new byte[]{(byte) 0x80};
	public final static byte[] TEST_RLP_EMPTY_LIST = new byte[]{(byte) 0xc0};
	public final static byte[] TEST_RLP_ELEMENT_INT_15 = new byte[] {0x0f};
	public final static byte[] TEST_RLP_ELEMENT_INT_1024 = new byte[] {(byte) 0x82,0x04,0x00};
	public final static byte[] TEST_RLP_LIST_SET3 = new byte[] {(byte) 0xc7,(byte) 0xc0,(byte) 0xc1,(byte) 0xc0,(byte) 0xc3,(byte) 0xc0,(byte) 0xc1,(byte) 0xc0};
	public final static byte[] TEST_RLP_ELEMENT_LARGESTRING = new byte[] {(byte) 0xb8,0x4E, 'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'};
	public final static byte[] TEST_RLP_LIST_LARGELIST = new byte[] {(byte) 0xf8,0x50,(byte) 0xb8,0x4E, 'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'};

	 @Test
	  public void checkTestDataBlock1346406Available() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth1346406.bin";
		String fileNameGenesis=classLoader.getResource("testdata/"+fileName).getFile();	
		assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameGenesis);
		File file = new File(fileNameGenesis);
		assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
		assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
	  }
	 
   @Test
   public void decodeRLPElementString() {
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilTest.TEST_RLP_ELEMENT_STRING);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPElement element = (RLPElement)elementObject;
	   assertArrayEquals("Indicator is correct", new byte[] {(byte) 0x83}, element.getIndicator());
	   assertArrayEquals("Raw data is correct", new byte[]{'d','o','g'},element.getRawData());
   }
   
   @Test
   public void decodeRLPListString() {
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilTest.TEST_RLP_LIST_STRING);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPList list = (RLPList)elementObject;
	   assertEquals("List of size 2",2,list.getRlpList().size());
	   RLPElement firstElement = (RLPElement) list.getRlpList().get(0);
	   assertArrayEquals("First element indicator is correct",new byte[]{(byte) 0x83},firstElement.getIndicator());
	   assertArrayEquals("First element raw data is correct",new byte[]{'c','a','t'},firstElement.getRawData());
	   RLPElement secondElement = (RLPElement) list.getRlpList().get(1);
	   assertArrayEquals("Second element indicator is correct",new byte[]{(byte) 0x83},secondElement.getIndicator());
	   assertArrayEquals("Second element raw data is correct",new byte[]{'d','o','g'},secondElement.getRawData());
   }
   
   @Test
   public void decodeRLPEmptyString() {
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilTest.TEST_RLP_EMPTY_STRING);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPElement element = (RLPElement)elementObject;
	   assertArrayEquals("Indicator is correct", new byte[] {(byte) 0x80}, element.getIndicator());
	   assertEquals("Raw data is zero", 0,element.getRawData().length);
   }
   
   @Test
   public void decodeRLPEmptyList() {
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilTest.TEST_RLP_EMPTY_LIST);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPList list = (RLPList)elementObject;
	   assertEquals("List of size 0",0,list.getRlpList().size());
   }
   
   @Test
   public void decodeRLPElementInt15() {
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilTest.TEST_RLP_ELEMENT_INT_15);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPElement element = (RLPElement)elementObject;
	   assertArrayEquals("Indicator is correct", new byte[] {(byte) 0x0f}, element.getIndicator());
	   assertEquals("Raw data has length 1", 1,element.getRawData().length);
	   assertEquals("Raw data contains the element 15", 15,element.getRawData()[0]);
   }
   
   @Test
   public void decodeRLPElementInt1024() {
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilTest.TEST_RLP_ELEMENT_INT_1024);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPElement element = (RLPElement)elementObject;
	   assertArrayEquals("Indicator is correct", new byte[] {(byte) 0x82}, element.getIndicator());
	   assertEquals("Raw data has length 2", 2,element.getRawData().length);
	   assertArrayEquals("Raw data contains the element 1024", ByteBuffer.allocate(2).putShort((short) 1024).array(),element.getRawData());
   }
   
   @Test
   public void decodeRLPListSet3() {
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilTest.TEST_RLP_LIST_SET3);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPList list = (RLPList)elementObject;
	   assertEquals("List of size 3",3,list.getRlpList().size());
	   RLPList firstList=(RLPList) list.getRlpList().get(0);
	   assertEquals("First list is empty",0,firstList.getRlpList().size());
	   RLPList secondList=(RLPList) list.getRlpList().get(1);
	   assertEquals("Second list contains one object",1,secondList.getRlpList().size());
	   RLPList secondListOneObject=(RLPList) secondList.getRlpList().get(0);
	   assertEquals("Second List First Element is an empty list",0,secondListOneObject.getRlpList().size());
	   RLPList thirdList = (RLPList) list.getRlpList().get(2);
	   assertEquals("Third list contains two objects",2,thirdList.getRlpList().size());
	   RLPList thirdListFirstObject = (RLPList) thirdList.getRlpList().get(0);
	   assertEquals("Third list first object is an empyt list",0,thirdListFirstObject.getRlpList().size());
	   RLPList thirdListSecondObject = (RLPList) thirdList.getRlpList().get(1);
	   assertEquals("Third list second object is a list of size 1",1,thirdListSecondObject.getRlpList().size());
	   RLPList thirdListSecondObjectListOneObject = (RLPList) thirdListSecondObject.getRlpList().get(0);
	   assertEquals("Third list second object list first item is an empty list",0,thirdListSecondObjectListOneObject.getRlpList().size());
   }
   
   @Test
   public void decodeRLPElementLargeString() {
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilTest.TEST_RLP_LIST_LARGELIST);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPList list = (RLPList)elementObject;
	   assertEquals("List contains one object",1,list.getRlpList().size());
	   RLPElement element = (RLPElement) list.getRlpList().get(0);
	   assertArrayEquals("List first object indicator is correct", new byte[] {(byte) 0xb8,(byte)0x4E}, element.getIndicator());
	   byte[] expectedRawData = new byte[]{'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'};
	   assertArrayEquals("List first object raw data is correct", expectedRawData,element.getRawData());
   }
   
   @Test
   public void getTransActionHashBlock1346406Transaction0() throws IOException, EthereumBlockReadException {
	   ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth1346406.bin";
		String fileNameBlock=classLoader.getResource("testdata/"+fileName).getFile();	
		File file = new File(fileNameBlock);
		boolean direct=false;
		FileInputStream fin = new FileInputStream(file);
		EthereumBlockReader ebr = null;
		try {
			ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
			EthereumBlock eblock = ebr.readBlock();
			List<EthereumTransaction> eTrans = eblock.getEthereumTransactions();
			EthereumTransaction trans0 = eTrans.get(0);
			byte[] expectedHash = new byte[] {(byte)0xe2,(byte)0x7e,(byte)0x92,(byte)0x88,(byte)0xe2,(byte)0x9c,(byte)0xc8,(byte)0xeb,(byte)0x78,(byte)0xf9,(byte)0xf7,(byte)0x68,(byte)0xd8,(byte)0x9b,(byte)0xf1,(byte)0xcd,(byte)0x4b,(byte)0x68,(byte)0xb7,(byte)0x15,(byte)0xa3,(byte)0x8b,(byte)0x95,(byte)0xd4,(byte)0x6d,(byte)0x77,(byte)0x86,(byte)0x18,(byte)0xcb,(byte)0x10,(byte)0x4d,(byte)0x58};
			assertArrayEquals("Block 1346406 Transaction 1 hash is correctly calculated",expectedHash,EthereumUtil.getTransactionHash(trans0));
			EthereumTransaction trans1 = eTrans.get(1);
			expectedHash = new byte[] {(byte)0x7a,(byte)0x23,(byte)0x2a,(byte)0xa2,(byte)0xae,(byte)0x6a,(byte)0x5e,(byte)0x1f,(byte)0x32,(byte)0xca,(byte)0x3a,(byte)0xc9,(byte)0x3f,(byte)0x4f,(byte)0xdb,(byte)0x77,(byte)0x98,(byte)0x3e,(byte)0x93,(byte)0x2b,(byte)0x38,(byte)0x09,(byte)0x93,(byte)0x56,(byte)0x44,(byte)0x42,(byte)0x08,(byte)0xc6,(byte)0x9d,(byte)0x40,(byte)0x86,(byte)0x81};
			assertArrayEquals("Block 1346406 Transaction 2 hash is correctly calculated",expectedHash,EthereumUtil.getTransactionHash(trans1));
			EthereumTransaction trans2 = eTrans.get(2);
			expectedHash = new byte[] {(byte)0x14,(byte)0x33,(byte)0xe3,(byte)0xcb,(byte)0x66,(byte)0x2f,(byte)0x66,(byte)0x8d,(byte)0x87,(byte)0xb8,(byte)0x35,(byte)0x55,(byte)0x34,(byte)0x5a,(byte)0x20,(byte)0xcc,(byte)0xf8,(byte)0x70,(byte)0x6f,(byte)0x25,(byte)0x21,(byte)0x49,(byte)0x18,(byte)0xe2,(byte)0xf8,(byte)0x1f,(byte)0xe3,(byte)0xd2,(byte)0x1c,(byte)0x9d,(byte)0x5b,(byte)0x23};
			assertArrayEquals("Block 1346406 Transaction 3 hash is correctly calculated",expectedHash,EthereumUtil.getTransactionHash(trans2));
			EthereumTransaction trans3 = eTrans.get(3);
			expectedHash = new byte[] {(byte)0x39,(byte)0x22,(byte)0xf7,(byte)0xf6,(byte)0x0a,(byte)0x33,(byte)0xa1,(byte)0x2d,(byte)0x13,(byte)0x9d,(byte)0x67,(byte)0xfa,(byte)0x53,(byte)0x30,(byte)0xdb,(byte)0xfd,(byte)0xba,(byte)0x42,(byte)0xa4,(byte)0xb7,(byte)0x67,(byte)0x29,(byte)0x6e,(byte)0xff,(byte)0x64,(byte)0x15,(byte)0xee,(byte)0xa3,(byte)0x2d,(byte)0x8a,(byte)0x7b,(byte)0x2b};
			assertArrayEquals("Block 1346406 Transaction 4 hash is correctly calculated",expectedHash,EthereumUtil.getTransactionHash(trans3));
			EthereumTransaction trans4 = eTrans.get(4);
			expectedHash = new byte[] {(byte)0xbb,(byte)0x7c,(byte)0xaa,(byte)0x23,(byte)0x38,(byte)0x5a,(byte)0x0f,(byte)0x73,(byte)0x75,(byte)0x3f,(byte)0x9e,(byte)0x28,(byte)0xd8,(byte)0xf0,(byte)0x60,(byte)0x2f,(byte)0xe2,(byte)0xe7,(byte)0x2d,(byte)0x87,(byte)0xe1,(byte)0xe0,(byte)0x95,(byte)0x52,(byte)0x75,(byte)0x28,(byte)0xd1,(byte)0x44,(byte)0x88,(byte)0x5d,(byte)0x6b,(byte)0x51};
			assertArrayEquals("Block 1346406 Transaction 5 hash is correctly calculated",expectedHash,EthereumUtil.getTransactionHash(trans4));
			EthereumTransaction trans5 = eTrans.get(5);
			expectedHash = new byte[] {(byte)0xbc,(byte)0xde,(byte)0x6f,(byte)0x49,(byte)0x84,(byte)0x2c,(byte)0x6d,(byte)0x73,(byte)0x8d,(byte)0x64,(byte)0x32,(byte)0x8f,(byte)0x78,(byte)0x09,(byte)0xb1,(byte)0xd4,(byte)0x9b,(byte)0xf0,(byte)0xff,(byte)0x3f,(byte)0xfa,(byte)0x46,(byte)0x0f,(byte)0xdd,(byte)0xd2,(byte)0x7f,(byte)0xd4,(byte)0x2b,(byte)0x7a,(byte)0x01,(byte)0xfc,(byte)0x9a};
			assertArrayEquals("Block 1346406 Transaction 6 hash is correctly calculated",expectedHash,EthereumUtil.getTransactionHash(trans5));
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
   }
	
}
