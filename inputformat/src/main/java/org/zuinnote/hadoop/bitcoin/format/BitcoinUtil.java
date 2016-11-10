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

package org.zuinnote.hadoop.bitcoin.format;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;


import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.util.Arrays;
import java.util.Date;

import javax.xml.bind.DatatypeConverter; // Hex Converter for configuration options

import java.security.MessageDigest; // needed for SHA2-256 calculation
import java.security.NoSuchAlgorithmException;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;


public class BitcoinUtil {

private static final Log LOG = LogFactory.getLog(BitcoinUtil.class.getName());

/**
* Converts a signed int to an unsigned (long)
*
* @param signedInt signed int that should be interpreted as unsigned
* 
* @return long corresponding to signed int
*
*/
public static long convertSignedIntToUnsigned(int signedInt) {
	long result=signedInt & 0x00000000ffffffffL;
	return result;
}


/**
*
* Converts an int to a byte array
*
* @param intToConvert int that should be converted into a byte array
*
* @return byte array corresponding to int
*
**/
public static byte[] convertIntToByteArray(int intToConvert) {
	return ByteBuffer.allocate(4).putInt(intToConvert).array();
}


/**
*
* Converts a long to a byte array
*
* @param longToConvert long that should be converted into a byte array
*
* @return byte array corresponding to long
*
**/
public static byte[] convertLongToByteArray(long longToConvert) {
	return ByteBuffer.allocate(8).putLong(longToConvert).array();
}




/**
* Converts a variable length integer (https://en.bitcoin.it/wiki/Protocol_documentation#Variable_length_integer) from a ByteBuffer to byte array
*
* @param byteBuffer Bytebuffer where to read from the variable length integer
* 
* @return byte[] of the variable length integer (including marker)
*
*/
public static byte[] convertVarIntByteBufferToByteArray(ByteBuffer byteBuffer) {
	// get the size
	byte originalVarIntSize=byteBuffer.get();
	byte varIntSize=getVarIntSize(originalVarIntSize);
	// reserveBuffer
	byte[] varInt=new byte[varIntSize];
	varInt[0]=originalVarIntSize;
	byteBuffer.get(varInt,1,varIntSize-1);
	return varInt;
}



/**
* Converts a variable length integer (https://en.bitcoin.it/wiki/Protocol_documentation#Variable_length_integer) from a ByteBuffer to long
*
* @param byteBuffer Bytebuffer where to read from the variable length integer
* 
* @return long corresponding to variable length integer. Please note that it is signed long and not unsigned long as int the Bitcoin specification. Should be in practice not relevant.
*
*/
public static long convertVarIntByteBufferToLong(ByteBuffer byteBuffer) {
	byte[] varIntByteArray=convertVarIntByteBufferToByteArray(byteBuffer);
	return getVarInt(varIntByteArray);
	
}


/**
* Converts a variable length integer (https://en.bitcoin.it/wiki/Protocol_documentation#Variable_length_integer) to long
*
* @param varInt byte array containing variable length integer
* 
* @return long corresponding to variable length integer
*
*/

public static long getVarInt(byte[] varInt) {
	long result=0;
	if (varInt.length==0) return result;
	int unsignedByte=varInt[0] & 0xFF;
	if (unsignedByte<0xFD) return unsignedByte;
	int intSize=0;
	if (unsignedByte==0xFD) intSize=3;
	else if (unsignedByte==0xFE) intSize=5;
	else if (unsignedByte==0XFF) intSize=9;
	byte[] rawDataInt=reverseByteArray(Arrays.copyOfRange(varInt, 1, intSize));
	ByteBuffer byteBuffer = ByteBuffer.wrap(rawDataInt);
	if (intSize==3) result=byteBuffer.getShort();
	else if (intSize==5) result=byteBuffer.getInt();
	else if (intSize==9) result=byteBuffer.getLong(); // Need to handle sign - available only in JDK8
	return result;
}

/**
* Determines size of a variable length integer (https://en.bitcoin.it/wiki/Protocol_documentation#Variable_length_integer)
*
* @param firstByteVarInt first byte of the variable integeer
* 
* @return byte with the size of the variable int (either 2, 3, 5 or 9) - does include the marker!
*
*/

public static byte getVarIntSize(byte firstByteVarInt) {
	int unsignedByte=firstByteVarInt & 0xFF;
	if (unsignedByte==0xFD) return 3;
	if (unsignedByte==0xFE) return 5;
	if (unsignedByte==0xFF) return 9;
	return 1; //<0xFD
}


/**
* Reads a size from a reversed byte order, such as block size in the block header
*
* @param byteSize byte array with a length of exactly 4 
* 
* @return size, returns 0 in case of invalid block size
*
*/

public static long getSize(byte[] byteSize) {
	if (byteSize.length!=4) return 0;
	ByteBuffer converterBuffer = ByteBuffer.wrap(byteSize);
	converterBuffer.order(ByteOrder.LITTLE_ENDIAN);
	return convertSignedIntToUnsigned(converterBuffer.getInt());
}


/**
* Reverses the order of the byte array
*
* @param inputByteArray array to be reversed
*
* @return inputByteArray in reversed order
*
**/
public static byte[] reverseByteArray(byte[] inputByteArray) {
	byte[] result=new byte[inputByteArray.length];
	for (int i=inputByteArray.length-1;i>=0;i--) {
		result[result.length-1-i]=inputByteArray[i];
	}
	return result;
}



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


/**
* Converts an int to a date
*
* @param dateInt timestamp in integer format 
*
* @return Date corresponding to dateInt
*
*/
public static Date convertIntToDate(int dateInt) {
    return new Date(dateInt*1000L);
}


/**
* Compares two Bitcoin magics
* 
* @param magic1 first magic
* @param magic2 second magics
*
* @return false, if do not match, true if match
*
*/

public static boolean compareMagics (byte[] magic1,byte[] magic2) {
	if (magic1.length!=magic2.length) return false;
	for (int i=0;i<magic1.length;i++) {
		if (magic1[i]!=magic2[i]) return false;
	}
	return true;	

}



/**
* Calculates the double SHA256-Hash of a transaction in little endian format. This could be used for certain analysis scenario where one want to investigate the referenced transaction used as an input for a Transaction. Furthermore, it can be used as a unique identifier of the transaction
*
*
* @param transaction The BitcoinTransaction of which we want to calculate the hash
*
* @return byte array containing the hash of the transaction. Note: This one can be compared to a prevTransactionHash. However, if you want to search for it in popular blockchain explorers then you need to apply the function BitcoinUtil.reverseByteArray to it!
*
*
* @throws java.io.IOException in case of errors reading from the InputStream
* @throws java.security.NoSuchAlgorithmException in case the hashing algorithm of the Bitcoin Blockchain is not supported by the JDK
*
*/
public static byte[] getTransactionHash(BitcoinTransaction transaction) throws NoSuchAlgorithmException, IOException{
	// convert transaction to byte array
	ByteArrayOutputStream transactionBAOS = new ByteArrayOutputStream();
	
	byte[] version = reverseByteArray(convertIntToByteArray(transaction.getVersion()));
	transactionBAOS.write(version);
	byte[] inCounter = transaction.getInCounter();
	transactionBAOS.write(inCounter);
	for (int i=0;i<transaction.getListOfInputs().size();i++) {
		transactionBAOS.write(transaction.getListOfInputs().get(i).getPrevTransactionHash());
		transactionBAOS.write(reverseByteArray(convertIntToByteArray(new Long(transaction.getListOfInputs().get(i).getPreviousTxOutIndex()).intValue())));
		transactionBAOS.write(transaction.getListOfInputs().get(i).getTxInScriptLength());
		transactionBAOS.write(transaction.getListOfInputs().get(i).getTxInScript());
		transactionBAOS.write(reverseByteArray(convertIntToByteArray(new Long(transaction.getListOfInputs().get(i).getSeqNo()).intValue())));
	}
	byte[] outCounter = transaction.getOutCounter();
	transactionBAOS.write(outCounter);
	for (int j=0;j<transaction.getListOfOutputs().size();j++) {
		transactionBAOS.write(reverseByteArray(convertLongToByteArray(transaction.getListOfOutputs().get(j).getValue())));		
		transactionBAOS.write(transaction.getListOfOutputs().get(j).getTxOutScriptLength());
		transactionBAOS.write(transaction.getListOfOutputs().get(j).getTxOutScript());
	}	
	byte[] lockTime=reverseByteArray(convertIntToByteArray(transaction.getLockTime()));
	transactionBAOS.write(lockTime);
	byte[] transactionByteArray= transactionBAOS.toByteArray();
	MessageDigest digest = MessageDigest.getInstance("SHA-256");
	byte[] firstRoundHash = digest.digest(transactionByteArray);
	byte[] secondRoundHash = digest.digest(firstRoundHash);
	byte[] finalHash = secondRoundHash;
	return finalHash;
}

	public static byte[] getBlockHash(BitcoinBlock block) throws NoSuchAlgorithmException, IOException {
		ByteArrayOutputStream blockBAOS = new ByteArrayOutputStream();
		String merkleTree = convertByteArrayToHexString(block.getHashMerkleRoot()).toLowerCase();
		blockBAOS.write(reverseByteArray(convertIntToByteArray(block.getVersion())));
		blockBAOS.write(block.getHashPrevBlock());
		blockBAOS.write(block.getHashMerkleRoot());
		blockBAOS.write(reverseByteArray(convertIntToByteArray(block.getTime())));
		blockBAOS.write(block.getBits());
		blockBAOS.write(reverseByteArray(convertIntToByteArray(block.getNonce())));
		byte[] blockByteArray = blockBAOS.toByteArray();
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		byte[] firstRoundHash = digest.digest(blockByteArray);
		byte[] secondRoundHash = digest.digest(firstRoundHash);
		byte[] finalHash = secondRoundHash;
		return reverseByteArray(finalHash);
    }


}
