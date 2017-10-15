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

package org.zuinnote.hadoop.ethereum.common;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.ByteBuffer;

import org.junit.Test;
import org.zuinnote.hadoop.ethereum.format.common.EthereumUtil;
import org.zuinnote.hadoop.ethereum.format.common.rlp.RLPElement;
import org.zuinnote.hadoop.ethereum.format.common.rlp.RLPList;
import org.zuinnote.hadoop.ethereum.format.common.rlp.RLPObject;

public class EthereumUtilTest {
	
	
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
	
}
