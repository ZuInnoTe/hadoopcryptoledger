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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zuinnote.hadoop.ethereum.format.common.rlp.RLPElement;
import org.zuinnote.hadoop.ethereum.format.common.rlp.RLPList;
import org.zuinnote.hadoop.ethereum.format.common.rlp.RLPObject;

/**
 *
 *
 */
public class EthereumUtil {
	public final static int RLP_OBJECTTYPE_INVALID = -1;
	public final static int RLP_OBJECTTYPE_ELEMENT = 0;
	public final static int RLP_OBJECTTYPE_LIST = 1;

	private static final Log LOG = LogFactory.getLog(EthereumUtil.class.getName());
	/** RLP functionality for Ethereum: https://github.com/ethereum/wiki/wiki/RLP **/


/**
 * Read RLP data from a Byte Buffer.
 *  
 * @param bb ByteBuffer from which to read the RLP data
 * @return RLPObject (e.g. RLPElement or RLPList) containing RLP data
 */
public static RLPObject rlpDecodeNextItem(ByteBuffer bb) {
	// detect object type
	RLPObject result=null;
    int objType = detectRLPObjectType(bb);
    switch (objType) {
	    case EthereumUtil.RLP_OBJECTTYPE_ELEMENT:
	    		result=EthereumUtil.decodeRLPElement(bb);
	    		break;
	    case EthereumUtil.RLP_OBJECTTYPE_LIST:
	    		result=EthereumUtil.decodeRLPList(bb);
	    		break;
	    default: LOG.error("Unknown object type");
    }
	return result;
}

/**
 * Detects the object type of an RLP encoded object. Note that it does not modify the read position in the ByteBuffer.
 * 
 * 
 * @param bb ByteBuffer that contains RLP encoded object
 * @return object type: EthereumUtil.RLP_OBJECTTYPE_ELEMENT or EthereumUtil.RLP_OBJECTTYPE_LIST or EthereumUtil.RLP_OBJECTTYPE_INVALID
 */
public static int detectRLPObjectType(ByteBuffer bb) {
	bb.mark();
	byte detector = bb.get();
	int unsignedDetector=detector & 0xFF;
	int result = EthereumUtil.RLP_OBJECTTYPE_INVALID;
	if (unsignedDetector<=0x7f) {
		result=EthereumUtil.RLP_OBJECTTYPE_ELEMENT;
	} else
	if ((unsignedDetector>=0x80) && (unsignedDetector<=0xb7)) {
		result=EthereumUtil.RLP_OBJECTTYPE_ELEMENT;
	} else
		if ((unsignedDetector>=0xb8) && (unsignedDetector<=0xbf)) {
			result=EthereumUtil.RLP_OBJECTTYPE_ELEMENT;
		}
	else 
		if ((unsignedDetector>=0xc0) && (unsignedDetector<=0xf7)) {
			result=EthereumUtil.RLP_OBJECTTYPE_LIST;
		} else
			if ((unsignedDetector>=0xf8) && (unsignedDetector<=0xff)) {
				result=EthereumUtil.RLP_OBJECTTYPE_LIST;
			}
			else {
				result=EthereumUtil.RLP_OBJECTTYPE_INVALID;
				LOG.error("Invalid RLP object type. Internal error or not RLP Data");
	}
	bb.reset();
	return result;
}

/*
 * Decodes an RLPElement from the given ByteBuffer
 * 
 *  @param bb Bytebuffer containing an RLPElement
 *  
 *  @return RLPElement in case the byte stream represents a valid RLPElement, null if not
 * 
 */
private static RLPElement decodeRLPElement(ByteBuffer bb) {
	RLPElement result=null;
	byte firstByte = bb.get();
	int firstByteUnsigned = firstByte & 0xFF;

	if (firstByteUnsigned <= 0x7F) {
		result=new RLPElement(new byte[] {firstByte},new byte[] {firstByte});
	} else if ((firstByteUnsigned>=0x80) && (firstByteUnsigned<=0xb7)) {
		// read indicator
		byte[] indicator=new byte[]{firstByte};
		int noOfBytes = firstByteUnsigned - 0x80;
		// read raw data
		byte[] rawData = new byte[noOfBytes];
		bb.get(rawData);
		result=new RLPElement(indicator,rawData);
	} else if ((firstByteUnsigned>=0xb8) && (firstByteUnsigned<=0xbf)) {
		// read size of indicator (size of the size)
		int NoOfBytesSize = firstByteUnsigned-0xb7;
		byte[] indicator = new byte[NoOfBytesSize+1];
		indicator[0]=firstByte;
		bb.get(indicator, 1, NoOfBytesSize);
		// read the size of the data
		byte[] rawDataNumber=Arrays.copyOfRange(indicator, 1, indicator.length);
		ByteBuffer byteBuffer = ByteBuffer.wrap(rawDataNumber);
		long noOfBytes = 0;
		if (indicator.length<3) { // byte
			noOfBytes=byteBuffer.get();
		} else if (indicator.length<4) { // short
			noOfBytes=byteBuffer.getShort();
		} else if (indicator.length<6) { // int
			noOfBytes=byteBuffer.getInt();
		} else if (indicator.length<10) { // long
			noOfBytes=byteBuffer.getLong();
		}

		// read the data
		byte[] rawData=new byte[(int) noOfBytes];
		bb.get(rawData);
		result= new RLPElement(indicator,rawData);
	} else {
		result=null;
	}
	return result;
}

/*
 * Decodes an RLPList from the given ByteBuffer. This list may contain further RLPList and Elements that are decoded as well
 * 
 *  @param bb Bytebuffer containing an RLPList
 *  
 *  @return RLPList in case the byte stream represents a valid RLPList, null if not
 * 
 */
private static RLPList decodeRLPList(ByteBuffer bb) {

	byte firstByte = bb.get();
	int firstByteUnsigned = firstByte & 0xFF;
	long payloadSize=0;
	if ((firstByteUnsigned>=0xc0) && (firstByteUnsigned<=0xf7)) {
		// length of the list in bytes
		payloadSize=firstByteUnsigned - 0xc0;
		
	} else if ((firstByteUnsigned>=0xf8) && (firstByteUnsigned<=0xff)) {
		// read size of indicator (size of the size)
		int NoOfBytesSize = firstByteUnsigned-0xf7;
		byte[] indicator = new byte[NoOfBytesSize+1];
		indicator[0]=firstByte;
		bb.get(indicator, 1, NoOfBytesSize);
		// read the size of the data
		byte[] rawDataNumber=Arrays.copyOfRange(indicator, 1, indicator.length);
		ByteBuffer byteBuffer = ByteBuffer.wrap(rawDataNumber);
		if (indicator.length<3) { // byte
			payloadSize=byteBuffer.get();
		} else if (indicator.length<4) { // short
			payloadSize=byteBuffer.getShort();
		} else if (indicator.length<6) { // int
			payloadSize=byteBuffer.getInt();
		} else if (indicator.length<10) { // long
			payloadSize=byteBuffer.getLong();
		}
	}
	ArrayList<RLPObject> payloadList=new ArrayList<>();
	if (payloadSize>0) {
		byte[] payload=new byte[(int) payloadSize];
		bb.get(payload);
		ByteBuffer payloadBB=ByteBuffer.wrap(payload);
		while(payloadBB.remaining()>0) {
			switch (EthereumUtil.detectRLPObjectType(payloadBB)) {
			 case EthereumUtil.RLP_OBJECTTYPE_ELEMENT:
		    		payloadList.add(EthereumUtil.decodeRLPElement(payloadBB));
		    		break;
		    case EthereumUtil.RLP_OBJECTTYPE_LIST:
		    		payloadList.add(EthereumUtil.decodeRLPList(payloadBB));
		    		break;
		    default: LOG.error("Unknown object type");
			
			}
			
		}
	} 
	return new RLPList(payloadList);
}



/** Data types conversions for Ethereum **/

public static byte convertToByte(RLPElement rpe) {
	return 0;
}

public static short convertToShort(RLPElement rpe) {
	return 0;
}

public static int convertToInt(RLPElement rpe) {
	return 0;
}

public static long convertToLong(RLPElement rpe) {
	return 0;
}
public static String convertToString(RLPElement rpe) {
	return null;
}

/** Hex functionality **/
/**
* Converts a Hex String to Byte Array. Only used for configuration not for parsing. Hex String is in format of xsd:hexBinary
*
* @param hexString String in Hex format.
*
* @return byte array corresponding to String in Hex format
*
*/
public static byte[] convertHexStringToByteArray(String hexString) {
    return DatatypeConverter.parseHexBinary(hexString);
}


/**
* Converts a Byte Array to Hex String. Only used for configuration not for parsing. Hex String is in format of xsd:hexBinary
*
* @param byteArray byte array to convert
*
* @return String in Hex format corresponding to byteArray
*
*/
public static String convertByteArrayToHexString(byte[] byteArray) {
    return DatatypeConverter.printHexBinary(byteArray);
}


}
