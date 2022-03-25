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


import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.ethereum.format.common.rlp.RLPElement;
import org.zuinnote.hadoop.ethereum.format.common.rlp.RLPList;
import org.zuinnote.hadoop.ethereum.format.common.rlp.RLPObject;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class EthereumUtilDecodeTest {

	public final static byte[] TEST_RLP_ELEMENT_INT_1024 = new byte[] {(byte) 0x82,0x04,0x00};
	public final static byte[] TEST_RLP_LIST_SET3 = new byte[] {(byte) 0xc7,(byte) 0xc0,(byte) 0xc1,(byte) 0xc0,(byte) 0xc3,(byte) 0xc0,(byte) 0xc1,(byte) 0xc0};
	public final static byte[] TEST_RLP_LIST_LARGE_LIST = new byte[] {(byte) 0xf8,0x50,(byte) 0xb8,0x4E, 'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'};

   @Test
   public void decodeRLPElementString() {
	   final byte[] TEST_RLP_ELEMENT_STRING = new byte[] {(byte) 0x83,'d','o','g'};
	   ByteBuffer wrapper = ByteBuffer.wrap(TEST_RLP_ELEMENT_STRING);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPElement element = (RLPElement)elementObject;
	   byte[] expectedIndicator = new byte[] {(byte) 0x83};
	   assertArrayEquals( expectedIndicator, element.getIndicator(),"Indicator is correct");
	   byte[] expectedRawData = new byte[]{'d','o','g'};
	   assertArrayEquals( expectedRawData,element.getRawData(),"Raw data is correct");
   }
   
   @Test
   public void decodeRLPListString() {
	   final byte[] TEST_RLP_LIST_STRING = new byte[] {(byte) 0xc8,(byte) 0x83,'c','a','t',(byte) 0x83,'d','o','g'};
	   ByteBuffer wrapper = ByteBuffer.wrap(TEST_RLP_LIST_STRING);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPList list = (RLPList)elementObject;
	   assertEquals(2,list.getRlpList().size(),"List of size 2");
	   RLPElement firstElement = (RLPElement) list.getRlpList().get(0);
	   byte[] expectedIndicator = new byte[]{(byte) 0x83};
	   assertArrayEquals(expectedIndicator,firstElement.getIndicator(),"First element indicator is correct");
	   byte[] expectedRawData=new byte[]{'c','a','t'};
	   assertArrayEquals(expectedRawData,firstElement.getRawData(),"First element raw data is correct");
	   RLPElement secondElement = (RLPElement) list.getRlpList().get(1);
	   expectedIndicator=new byte[]{(byte) 0x83};
	   assertArrayEquals(expectedIndicator,secondElement.getIndicator(),"Second element indicator is correct");
	   expectedRawData=new byte[]{'d','o','g'};
	   assertArrayEquals(expectedRawData,secondElement.getRawData(),"Second element raw data is correct");
   }
   
   @Test
   public void decodeRLPEmptyString() {
	   final byte[] TEST_RLP_EMPTY_STRING = new byte[]{(byte) 0x80};
	   ByteBuffer wrapper = ByteBuffer.wrap(TEST_RLP_EMPTY_STRING);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPElement element = (RLPElement)elementObject;
	   byte[] expectedIndicator = new byte[] {(byte) 0x80};
	   assertArrayEquals( expectedIndicator, element.getIndicator(),"Indicator is correct");
	   assertEquals( 0,element.getRawData().length,"Raw data is zero");
   }
   
   @Test
   public void decodeRLPEmptyList() {
	   final byte[] TEST_RLP_EMPTY_LIST = new byte[]{(byte) 0xc0};
	   ByteBuffer wrapper = ByteBuffer.wrap(TEST_RLP_EMPTY_LIST);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPList list = (RLPList)elementObject;
	   assertEquals(0,list.getRlpList().size(),"List of size 0");
   }
   
   @Test
   public void decodeRLPElementInt15() {
	   final byte[] TEST_RLP_ELEMENT_INT_15 = new byte[] {0x0f};
	   ByteBuffer wrapper = ByteBuffer.wrap(TEST_RLP_ELEMENT_INT_15);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPElement element = (RLPElement)elementObject;
	   byte[] expectedIndicator = new byte[] {(byte) 0x0f};
	   assertArrayEquals( expectedIndicator, element.getIndicator(),"Indicator is correct");
	   assertEquals( 1,element.getRawData().length,"Raw data has length 1");
	   assertEquals( 15,element.getRawData()[0],"Raw data contains the element 15");
   }
   
   @Test
   public void decodeRLPElementInt1024() {
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilDecodeTest.TEST_RLP_ELEMENT_INT_1024);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPElement element = (RLPElement)elementObject;
	   byte[] expectedIndicator = new byte[] {(byte) 0x82};
	   assertArrayEquals( expectedIndicator, element.getIndicator(),"Indicator is correct");
	   assertEquals( 2,element.getRawData().length,"Raw data has length 2");
	   assertArrayEquals( ByteBuffer.allocate(2).putShort((short) 1024).array(),element.getRawData(),"Raw data contains the element 1024");
   }
   
   @Test
   public void decodeRLPListSet3() {
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilDecodeTest.TEST_RLP_LIST_SET3);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPList list = (RLPList)elementObject;
	   assertEquals(3,list.getRlpList().size(),"List of size 3");
	   RLPList firstList=(RLPList) list.getRlpList().get(0);
	   assertEquals(0,firstList.getRlpList().size(),"First list is empty");
	   RLPList secondList=(RLPList) list.getRlpList().get(1);
	   assertEquals(1,secondList.getRlpList().size(),"Second list contains one object");
	   RLPList secondListOneObject=(RLPList) secondList.getRlpList().get(0);
	   assertEquals(0,secondListOneObject.getRlpList().size(),"Second List First Element is an empty list");
	   RLPList thirdList = (RLPList) list.getRlpList().get(2);
	   assertEquals(2,thirdList.getRlpList().size(),"Third list contains two objects");
	   RLPList thirdListFirstObject = (RLPList) thirdList.getRlpList().get(0);
	   assertEquals(0,thirdListFirstObject.getRlpList().size(),"Third list first object is an empyt list");
	   RLPList thirdListSecondObject = (RLPList) thirdList.getRlpList().get(1);
	   assertEquals(1,thirdListSecondObject.getRlpList().size(),"Third list second object is a list of size 1");
	   RLPList thirdListSecondObjectListOneObject = (RLPList) thirdListSecondObject.getRlpList().get(0);
	   assertEquals(0,thirdListSecondObjectListOneObject.getRlpList().size(),"Third list second object list first item is an empty list");
   }
   
   @Test
   public void decodeRLPElementLargeString() {
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilDecodeTest.TEST_RLP_LIST_LARGE_LIST);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPList list = (RLPList)elementObject;
	   assertEquals(1,list.getRlpList().size(),"List contains one object");
	   RLPElement element = (RLPElement) list.getRlpList().get(0);
	   byte[] expectedIndicator = new byte[] {(byte) 0xb8,(byte)0x4E};
	   assertArrayEquals( expectedIndicator, element.getIndicator(),"List first object indicator is correct");
	   byte[] expectedRawData = new byte[]{'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'};
	   assertArrayEquals( expectedRawData,element.getRawData(),"List first object raw data is correct");
   }
	
}
