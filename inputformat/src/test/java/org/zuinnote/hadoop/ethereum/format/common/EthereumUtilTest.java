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


import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.ethereum.format.common.EthereumUtil;
import org.zuinnote.hadoop.ethereum.format.common.rlp.RLPElement;
import org.zuinnote.hadoop.ethereum.format.common.rlp.RLPList;
import org.zuinnote.hadoop.ethereum.format.common.rlp.RLPObject;
import org.zuinnote.hadoop.ethereum.format.exception.EthereumBlockReadException;

public class EthereumUtilTest {
	static final int DEFAULT_BUFFERSIZE=64*1024;
	static final int DEFAULT_MAXSIZE_ETHEREUMBLOCK=1 * 1024 * 1024;
	public final static int CHAIN_ID=1;
	
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
		assertNotNull(fileNameGenesis,"Test Data File \""+fileName+"\" is not null in resource path");
		File file = new File(fileNameGenesis);
		assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
		assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
	  }
	 
   @Test
   public void decodeRLPElementString() {
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilTest.TEST_RLP_ELEMENT_STRING);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPElement element = (RLPElement)elementObject;
	   byte[] expectedIndicator = new byte[] {(byte) 0x83};
	   assertArrayEquals( expectedIndicator, element.getIndicator(),"Indicator is correct");
	   byte[] expectedRawData = new byte[]{'d','o','g'};
	   assertArrayEquals( expectedRawData,element.getRawData(),"Raw data is correct");
   }
   
   @Test
   public void decodeRLPListString() {
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilTest.TEST_RLP_LIST_STRING);
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
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilTest.TEST_RLP_EMPTY_STRING);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPElement element = (RLPElement)elementObject;
	   byte[] expectedIndicator = new byte[] {(byte) 0x80};
	   assertArrayEquals( expectedIndicator, element.getIndicator(),"Indicator is correct");
	   assertEquals( 0,element.getRawData().length,"Raw data is zero");
   }
   
   @Test
   public void decodeRLPEmptyList() {
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilTest.TEST_RLP_EMPTY_LIST);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPList list = (RLPList)elementObject;
	   assertEquals(0,list.getRlpList().size(),"List of size 0");
   }
   
   @Test
   public void decodeRLPElementInt15() {
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilTest.TEST_RLP_ELEMENT_INT_15);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPElement element = (RLPElement)elementObject;
	   byte[] expectedIndicator = new byte[] {(byte) 0x0f};
	   assertArrayEquals( expectedIndicator, element.getIndicator(),"Indicator is correct");
	   assertEquals( 1,element.getRawData().length,"Raw data has length 1");
	   assertEquals( 15,element.getRawData()[0],"Raw data contains the element 15");
   }
   
   @Test
   public void decodeRLPElementInt1024() {
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilTest.TEST_RLP_ELEMENT_INT_1024);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPElement element = (RLPElement)elementObject;
	   byte[] expectedIndicator = new byte[] {(byte) 0x82};
	   assertArrayEquals( expectedIndicator, element.getIndicator(),"Indicator is correct");
	   assertEquals( 2,element.getRawData().length,"Raw data has length 2");
	   assertArrayEquals( ByteBuffer.allocate(2).putShort((short) 1024).array(),element.getRawData(),"Raw data contains the element 1024");
   }
   
   @Test
   public void decodeRLPListSet3() {
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilTest.TEST_RLP_LIST_SET3);
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
	   ByteBuffer wrapper = ByteBuffer.wrap(EthereumUtilTest.TEST_RLP_LIST_LARGELIST);
	   RLPObject elementObject = EthereumUtil.rlpDecodeNextItem(wrapper);
	   RLPList list = (RLPList)elementObject;
	   assertEquals(1,list.getRlpList().size(),"List contains one object");
	   RLPElement element = (RLPElement) list.getRlpList().get(0);
	   byte[] expectedIndicator = new byte[] {(byte) 0xb8,(byte)0x4E};
	   assertArrayEquals( expectedIndicator, element.getIndicator(),"List first object indicator is correct");
	   byte[] expectedRawData = new byte[]{'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'};
	   assertArrayEquals( expectedRawData,element.getRawData(),"List first object raw data is correct");
   }
   
   @Test
   public void encodeRLPElementLargeByteArray() {
	   String hexString="0A19B14A0000000000000000000000008F8221AFBB33998D8584A2B05749BA73C37A938A000000000000000000000000000000000000000000000028A857425466F80000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004FEFA17B72400000000000000000000000000000000000000000000000000000000000000496606000000000000000000000000000000000000000000000000000000000B4594E6000000000000000000000000439F3E3AEB991155923B2E9D79B40169C38238C6000000000000000000000000000000000000000000000000000000000000001C6CAFDE39C9387CA142932617D966B4EEDA67ADE303604D3D7F57D796393324225DF009CCAA166300AC48C95013B8159F545CF11CC17B2BB23903FE1E5E02DD3D0000000000000000000000000000000000000000000000284041E810FF8A0000";
	   byte[] byteArray=EthereumUtil.convertHexStringToByteArray(hexString);
	   byte[] rlpEncode = EthereumUtil.encodeRLPElement(byteArray);
	   String rlpHexString = "B90164"+hexString;
	   assertEquals(rlpHexString,EthereumUtil.convertByteArrayToHexString(rlpEncode));
   }
   @Test
   public void encodeRLPElementLargeByteArray2() {
	   String hexString="6060604052341561000F57600080FD5B6040516060806105B08339810160405280805191906020018051919060200180519150505B600160A060020A038316151561004957600080FD5B60008054600160A060020A03808616600160A060020A0319928316179092556001805485841690831617905560028054928416929091169190911790555B5050505B6105168061009A6000396000F300606060405236156100675763FFFFFFFF60E060020A6000350416631072CBEA81146100A55780636C3B58E0146100C957806373FFD5B7146100F65780637822ED491461010E5780638DA5CB5B1461013D578063DC5F346B1461016C578063F444FDD814610199575B5B6000341161007557600080FD5B600154600160A060020A03163460405160006040518083038185876187965A03F19250505015156100A257FE5B5B005B34156100B057600080FD5B6100A2600160A060020A03600435166024356101C8565B005B34156100D457600080FD5B6100A2600160A060020A036004351660243560443560643560843561033A565B005B341561010157600080FD5B6100A26004356103AA565B005B341561011957600080FD5B61012161044D565B604051600160A060020A03909116815260200160405180910390F35B341561014857600080FD5B61012161045C565B604051600160A060020A03909116815260200160405180910390F35B341561017757600080FD5B6100A2600160A060020A036004351660243560443560643560843561046B565B005B34156101A457600080FD5B6101216104DB565B604051600160A060020A03909116815260200160405180910390F35B60025460009033600160A060020A03908116911614806101F6575060005433600160A060020A039081169116145B151561020157600080FD5B600160A060020A038316151561021357FE5B82600160A060020A03166370A082313060006040516020015260405160E060020A63FFFFFFFF8416028152600160A060020A039091166004820152602401602060405180830381600087803B151561026A57600080FD5B6102C65A03F1151561027B57600080FD5B50505060405180519150506000811180156102965750600082115B80156102A25750818110155B15156102AD57600080FD5B600154600160A060020A038085169163A9059CBB91168460006040516020015260405160E060020A63FFFFFFFF8516028152600160A060020A0390921660048301526024820152604401602060405180830381600087803B151561031057600080FD5B6102C65A03F1151561032157600080FD5B50505060405180519050151561033357FE5B5B5B505050565B60005433600160A060020A0390811691161461035557600080FD5B600160A060020A038516151561036A57600080FD5B80828486081461037957600080FD5B6001805473FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF1916600160A060020A0387161790555B5B5050505050565B60025433600160A060020A03908116911614806103D5575060005433600160A060020A039081169116145B15156103E057600080FD5B600030600160A060020A0316311180156103FA5750600081115B801561041057508030600160A060020A03163110155B151561041B57600080FD5B600154600160A060020A03168160405160006040518083038185876187965A03F192505050151561044857FE5B5B5B50565B600154600160A060020A031681565B600054600160A060020A031681565B60005433600160A060020A0390811691161461048657600080FD5B600160A060020A038516151561049B57600080FD5B8082848608146104AA57600080FD5B6002805473FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF1916600160A060020A0387161790555B5B5050505050565B600254600160A060020A0316815600A165627A7A72305820CF04E4D1DA97DCE96924EC4676A82FC1B7E1DF58CA22EC7328A5718A498BF8120029000000000000000000000000FE30E28B9DB06769D1D5F263905CDA31C4B2E0950000000000000000000000007B74C19124A9CA92C6141A2ED5F92130FC2791F20000000000000000000000007250ABCACDF1B5E19EC216619EA2614DC2FF6B7A";
	   byte[] byteArray=EthereumUtil.convertHexStringToByteArray(hexString);
	   byte[] rlpEncode = EthereumUtil.encodeRLPElement(byteArray);
	   String rlpHexString = "B90610"+hexString;
	   assertEquals(rlpHexString,EthereumUtil.convertByteArrayToHexString(rlpEncode));
   }
   
   @Test
   public void calculateChainIdBlock1346406() throws IOException, EthereumBlockReadException {
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
				assertNull(EthereumUtil.calculateChainId(trans0),"Block 1346406 Transaction 1 is Ethereum MainNet");
				EthereumTransaction trans1 = eTrans.get(1);
				assertNull(EthereumUtil.calculateChainId(trans1),"Block 1346406 Transaction 2 is Ethereum MainNet");
				EthereumTransaction trans2 = eTrans.get(2);
				assertNull(EthereumUtil.calculateChainId(trans2),"Block 1346406 Transaction 3 is Ethereum MainNet");
				EthereumTransaction trans3 = eTrans.get(3);
				assertNull(EthereumUtil.calculateChainId(trans3),"Block 1346406 Transaction 4 is Ethereum MainNet");
				EthereumTransaction trans4 = eTrans.get(4);
				assertNull(EthereumUtil.calculateChainId(trans4),"Block 1346406 Transaction 5 is Ethereum MainNet");
				EthereumTransaction trans5 = eTrans.get(5);
				assertNull(EthereumUtil.calculateChainId(trans5),"Block 1346406 Transaction 6 is Ethereum MainNet");
			} finally {
				if (ebr!=null) {
					ebr.close();
				}
			}
   }
   
   @Test
   public void getTransActionHashBlock1346406() throws IOException, EthereumBlockReadException {
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
			assertArrayEquals(expectedHash,EthereumUtil.getTransactionHash(trans0),"Block 1346406 Transaction 1 hash is correctly calculated");
			EthereumTransaction trans1 = eTrans.get(1);
			expectedHash = new byte[] {(byte)0x7a,(byte)0x23,(byte)0x2a,(byte)0xa2,(byte)0xae,(byte)0x6a,(byte)0x5e,(byte)0x1f,(byte)0x32,(byte)0xca,(byte)0x3a,(byte)0xc9,(byte)0x3f,(byte)0x4f,(byte)0xdb,(byte)0x77,(byte)0x98,(byte)0x3e,(byte)0x93,(byte)0x2b,(byte)0x38,(byte)0x09,(byte)0x93,(byte)0x56,(byte)0x44,(byte)0x42,(byte)0x08,(byte)0xc6,(byte)0x9d,(byte)0x40,(byte)0x86,(byte)0x81};
			assertArrayEquals(expectedHash,EthereumUtil.getTransactionHash(trans1),"Block 1346406 Transaction 2 hash is correctly calculated");
			EthereumTransaction trans2 = eTrans.get(2);
			expectedHash = new byte[] {(byte)0x14,(byte)0x33,(byte)0xe3,(byte)0xcb,(byte)0x66,(byte)0x2f,(byte)0x66,(byte)0x8d,(byte)0x87,(byte)0xb8,(byte)0x35,(byte)0x55,(byte)0x34,(byte)0x5a,(byte)0x20,(byte)0xcc,(byte)0xf8,(byte)0x70,(byte)0x6f,(byte)0x25,(byte)0x21,(byte)0x49,(byte)0x18,(byte)0xe2,(byte)0xf8,(byte)0x1f,(byte)0xe3,(byte)0xd2,(byte)0x1c,(byte)0x9d,(byte)0x5b,(byte)0x23};
			assertArrayEquals(expectedHash,EthereumUtil.getTransactionHash(trans2),"Block 1346406 Transaction 3 hash is correctly calculated");
			EthereumTransaction trans3 = eTrans.get(3);
			expectedHash = new byte[] {(byte)0x39,(byte)0x22,(byte)0xf7,(byte)0xf6,(byte)0x0a,(byte)0x33,(byte)0xa1,(byte)0x2d,(byte)0x13,(byte)0x9d,(byte)0x67,(byte)0xfa,(byte)0x53,(byte)0x30,(byte)0xdb,(byte)0xfd,(byte)0xba,(byte)0x42,(byte)0xa4,(byte)0xb7,(byte)0x67,(byte)0x29,(byte)0x6e,(byte)0xff,(byte)0x64,(byte)0x15,(byte)0xee,(byte)0xa3,(byte)0x2d,(byte)0x8a,(byte)0x7b,(byte)0x2b};
			assertArrayEquals(expectedHash,EthereumUtil.getTransactionHash(trans3),"Block 1346406 Transaction 4 hash is correctly calculated");
			EthereumTransaction trans4 = eTrans.get(4);
			expectedHash = new byte[] {(byte)0xbb,(byte)0x7c,(byte)0xaa,(byte)0x23,(byte)0x38,(byte)0x5a,(byte)0x0f,(byte)0x73,(byte)0x75,(byte)0x3f,(byte)0x9e,(byte)0x28,(byte)0xd8,(byte)0xf0,(byte)0x60,(byte)0x2f,(byte)0xe2,(byte)0xe7,(byte)0x2d,(byte)0x87,(byte)0xe1,(byte)0xe0,(byte)0x95,(byte)0x52,(byte)0x75,(byte)0x28,(byte)0xd1,(byte)0x44,(byte)0x88,(byte)0x5d,(byte)0x6b,(byte)0x51};
			assertArrayEquals(expectedHash,EthereumUtil.getTransactionHash(trans4),"Block 1346406 Transaction 5 hash is correctly calculated");
			EthereumTransaction trans5 = eTrans.get(5);
			expectedHash = new byte[] {(byte)0xbc,(byte)0xde,(byte)0x6f,(byte)0x49,(byte)0x84,(byte)0x2c,(byte)0x6d,(byte)0x73,(byte)0x8d,(byte)0x64,(byte)0x32,(byte)0x8f,(byte)0x78,(byte)0x09,(byte)0xb1,(byte)0xd4,(byte)0x9b,(byte)0xf0,(byte)0xff,(byte)0x3f,(byte)0xfa,(byte)0x46,(byte)0x0f,(byte)0xdd,(byte)0xd2,(byte)0x7f,(byte)0xd4,(byte)0x2b,(byte)0x7a,(byte)0x01,(byte)0xfc,(byte)0x9a};
			assertArrayEquals(expectedHash,EthereumUtil.getTransactionHash(trans5),"Block 1346406 Transaction 6 hash is correctly calculated");
		} finally {
			if (ebr!=null) {
				ebr.close();
			}
		}
   }
   
   @Test
   public void getTransActionSendAddressBlock1346406() throws IOException, EthereumBlockReadException {
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
      byte[] expectedSentAddress = new byte[] {(byte)0x39,(byte)0x42,(byte)0x4b,(byte)0xd2,(byte)0x8a,(byte)0x22,(byte)0x23,(byte)0xda,(byte)0x3e,(byte)0x14,(byte)0xbf,(byte)0x79,(byte)0x3c,(byte)0xf7,(byte)0xf8,(byte)0x20,(byte)0x8e,(byte)0xe9,(byte)0x98,(byte)0x0a};
      assertArrayEquals(expectedSentAddress,EthereumUtil.getSendAddress(trans0,EthereumUtilTest.CHAIN_ID),"Block 1346406 Transaction 1 send address is correctly calculated");
      EthereumTransaction trans1 = eTrans.get(1);
      expectedSentAddress = new byte[] {(byte)0x4b,(byte)0xb9,(byte)0x60,(byte)0x91,(byte)0xee,(byte)0x9d,(byte)0x80,(byte)0x2e,(byte)0xd0,(byte)0x39,(byte)0xc4,(byte)0xd1,(byte)0xa5,(byte)0xf6,(byte)0x21,(byte)0x6f,(byte)0x90,(byte)0xf8,(byte)0x1b,(byte)0x01};
      assertArrayEquals(expectedSentAddress,EthereumUtil.getSendAddress(trans1,EthereumUtilTest.CHAIN_ID),"Block 1346406 Transaction 2 send address is correctly calculated");
      EthereumTransaction trans2 = eTrans.get(2);
      expectedSentAddress = new byte[] {(byte)0x63,(byte)0xa9,(byte)0x97,(byte)0x5b,(byte)0xa3,(byte)0x1b,(byte)0x0b,(byte)0x96,(byte)0x26,(byte)0xb3,(byte)0x43,(byte)0x00,(byte)0xf7,(byte)0xf6,(byte)0x27,(byte)0x14,(byte)0x7d,(byte)0xf1,(byte)0xf5,(byte)0x26};
      assertArrayEquals(expectedSentAddress,EthereumUtil.getSendAddress(trans2,EthereumUtilTest.CHAIN_ID),"Block 1346406 Transaction 3 send address is correctly calculated");
      EthereumTransaction trans3 = eTrans.get(3);
      expectedSentAddress = new byte[] {(byte)0x63,(byte)0xa9,(byte)0x97,(byte)0x5b,(byte)0xa3,(byte)0x1b,(byte)0x0b,(byte)0x96,(byte)0x26,(byte)0xb3,(byte)0x43,(byte)0x00,(byte)0xf7,(byte)0xf6,(byte)0x27,(byte)0x14,(byte)0x7d,(byte)0xf1,(byte)0xf5,(byte)0x26};
     assertArrayEquals(expectedSentAddress,EthereumUtil.getSendAddress(trans3,EthereumUtilTest.CHAIN_ID),"Block 1346406 Transaction 4 send address is correctly calculated");
      EthereumTransaction trans4 = eTrans.get(4);
      expectedSentAddress = new byte[] {(byte)0x63,(byte)0xa9,(byte)0x97,(byte)0x5b,(byte)0xa3,(byte)0x1b,(byte)0x0b,(byte)0x96,(byte)0x26,(byte)0xb3,(byte)0x43,(byte)0x00,(byte)0xf7,(byte)0xf6,(byte)0x27,(byte)0x14,(byte)0x7d,(byte)0xf1,(byte)0xf5,(byte)0x26};
      assertArrayEquals(expectedSentAddress,EthereumUtil.getSendAddress(trans4,EthereumUtilTest.CHAIN_ID),"Block 1346406 Transaction 5 send address is correctly calculated");
      EthereumTransaction trans5 = eTrans.get(5);
      expectedSentAddress = new byte[] {(byte)0x63,(byte)0xa9,(byte)0x97,(byte)0x5b,(byte)0xa3,(byte)0x1b,(byte)0x0b,(byte)0x96,(byte)0x26,(byte)0xb3,(byte)0x43,(byte)0x00,(byte)0xf7,(byte)0xf6,(byte)0x27,(byte)0x14,(byte)0x7d,(byte)0xf1,(byte)0xf5,(byte)0x26};
      assertArrayEquals(expectedSentAddress,EthereumUtil.getSendAddress(trans5,EthereumUtilTest.CHAIN_ID),"Block 1346406 Transaction 6 send address is correctly calculated");
    } finally {
      if (ebr!=null) {
        ebr.close();
      }
    }
   }
   
	
}
