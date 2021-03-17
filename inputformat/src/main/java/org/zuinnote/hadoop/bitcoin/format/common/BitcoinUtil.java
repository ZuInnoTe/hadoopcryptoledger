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

package org.zuinnote.hadoop.bitcoin.format.common;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Date;

import javax.xml.bind.DatatypeConverter; // Hex Converter for configuration options

import java.security.MessageDigest; // needed for SHA2-256 calculation
import java.security.NoSuchAlgorithmException;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;


public class BitcoinUtil {

private static final Log LOG = LogFactory.getLog(BitcoinUtil.class.getName());

private BitcoinUtil() {
}

/**
* Converts a signed int to an unsigned (long)
*
* @param signedInt signed int that should be interpreted as unsigned
*
* @return long corresponding to signed int
*
*/
public static long convertSignedIntToUnsigned(int signedInt) {
	return signedInt & 0x00000000ffffffffL;
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
*
* Converts a Big Integer to a byte array
*
* @param bigIntegerToConvert BigInteger that should be converted into a byte array
* @param exactArraySize exact size of array
* @return byte array corresponding to BigInteger
*
**/
public static byte[] convertBigIntegerToByteArray(BigInteger bigIntegerToConvert,int exactArraySize) {
	if ((bigIntegerToConvert==null) || (bigIntegerToConvert.signum()==-1))  {// negative
		return null;
	}
	 byte[] tempResult = bigIntegerToConvert.toByteArray();
	 byte [] result = new byte[exactArraySize];
	 int removeSign=0;
	 if ((tempResult.length>1) && (tempResult[0]==0)) { // remove sign
		removeSign=1;
	 }
	 byte[] reverseTempResult = BitcoinUtil.reverseByteArray(tempResult);
	 for (int i=0;i<result.length;i++) {
		 if (i<reverseTempResult.length-removeSign) {
			 result[i]=reverseTempResult[i];
		 }
	 }
	 return result;
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
* Converts a variable length integer (https://en.bitcoin.it/wiki/Protocol_documentation#Variable_length_integer) to BigInteger
*
* @param varInt byte array containing variable length integer
*
* @return BigInteger corresponding to variable length integer
*
*/

public static BigInteger getVarIntBI(byte[] varInt) {
	BigInteger result=BigInteger.ZERO;
	if (varInt.length==0) {
		 return result;
	}
	int unsignedByte=varInt[0] & 0xFF;
	if (unsignedByte<0xFD) {
		return new BigInteger(new byte[] {(byte) unsignedByte});
	}
	int intSize=0;
	if (unsignedByte==0xFD) {
		intSize=3;
	}
	else if (unsignedByte==0xFE) {
		intSize=5;
	}
	else if (unsignedByte==0XFF) {
		intSize=9;
	}
	byte[] rawDataInt=reverseByteArray(Arrays.copyOfRange(varInt, 1, intSize));
	return new BigInteger(rawDataInt);
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
	if (varInt.length==0) {
		 return result;
	}
	int unsignedByte=varInt[0] & 0xFF;
	if (unsignedByte<0xFD) {
		return unsignedByte;
	}
	int intSize=0;
	if (unsignedByte==0xFD) {
		intSize=3;
	}
	else if (unsignedByte==0xFE) {
		intSize=5;
	}
	else if (unsignedByte==0XFF) {
		intSize=9;
	}
	byte[] rawDataInt=reverseByteArray(Arrays.copyOfRange(varInt, 1, intSize));
	ByteBuffer byteBuffer = ByteBuffer.wrap(rawDataInt);
	if (intSize==3) {
		 result=byteBuffer.getShort();
	}
	else if (intSize==5) {
		result=byteBuffer.getInt();
	}
	else if (intSize==9) {
		result=byteBuffer.getLong(); // Need to handle sign - available only in JDK8
	}
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
	if (unsignedByte==0xFD) {
		 return 3;
	}
	if (unsignedByte==0xFE) {
		return 5;
	}
	if (unsignedByte==0xFF) {
		return 9;
	}
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
	if (byteSize.length!=4) {
		return 0;
	}
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
	if (magic1.length!=magic2.length) {
		return false;
	}
	for (int i=0;i<magic1.length;i++) {
		if (magic1[i]!=magic2[i]) {
			 return false;
		}
	}
	return true;

}



/**
* Calculates the double SHA256-Hash of a transaction in little endian format. This could be used for certain analysis scenario where one want to investigate the referenced transaction used as an input for a Transaction. Furthermore, it can be used as a unique identifier of the transaction
*
* It corresponds to the Bitcoin specification of txid (https://bitcoincore.org/en/segwit_wallet_dev/)
*
* @param transaction The BitcoinTransaction of which we want to calculate the hash
*
* @return byte array containing the hash of the transaction. Note: This one can be compared to a prevTransactionHash. However, if you want to search for it in popular blockchain explorers then you need to apply the function BitcoinUtil.reverseByteArray to it!
*
*
* @throws java.io.IOException in case of errors reading from the InputStream
*
*/
public static byte[] getTransactionHash(BitcoinTransaction transaction) throws IOException{
	// convert transaction to byte array
	ByteArrayOutputStream transactionBAOS = new ByteArrayOutputStream();

	byte[] version = reverseByteArray(convertIntToByteArray((int)transaction.getVersion()));
	transactionBAOS.write(version);
	byte[] inCounter = transaction.getInCounter();
	transactionBAOS.write(inCounter);
	for (int i=0;i<transaction.getListOfInputs().size();i++) {
		transactionBAOS.write(transaction.getListOfInputs().get(i).getPrevTransactionHash());
		transactionBAOS.write(reverseByteArray(convertIntToByteArray((int)(transaction.getListOfInputs().get(i).getPreviousTxOutIndex()))));
		transactionBAOS.write(transaction.getListOfInputs().get(i).getTxInScriptLength());
		transactionBAOS.write(transaction.getListOfInputs().get(i).getTxInScript());
		transactionBAOS.write(reverseByteArray(convertIntToByteArray((int)(transaction.getListOfInputs().get(i).getSeqNo()))));
	}
	byte[] outCounter = transaction.getOutCounter();
	transactionBAOS.write(outCounter);
	for (int j=0;j<transaction.getListOfOutputs().size();j++) {
		transactionBAOS.write(convertBigIntegerToByteArray(transaction.getListOfOutputs().get(j).getValue(),8));
		transactionBAOS.write(transaction.getListOfOutputs().get(j).getTxOutScriptLength());
		transactionBAOS.write(transaction.getListOfOutputs().get(j).getTxOutScript());
	}
	byte[] lockTime=reverseByteArray(convertIntToByteArray((int)transaction.getLockTime()));
	transactionBAOS.write(lockTime);
	byte[] transactionByteArray= transactionBAOS.toByteArray();
	byte[] firstRoundHash;
	byte[] secondRoundHash;
	try {
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		firstRoundHash = digest.digest(transactionByteArray);
		secondRoundHash = digest.digest(firstRoundHash);
	} catch (NoSuchAlgorithmException nsae) {
		LOG.error(nsae);
		return new byte[0];
	}
	return secondRoundHash;
}


/**
* Calculates the double SHA256-Hash of a transaction in little endian format. It serve as a unique identifier of a transaction, but cannot be used to link the outputs of other transactions as input
*
* It corresponds to the Bitcoin specification of wtxid (https://bitcoincore.org/en/segwit_wallet_dev/)
*
* @param transaction The BitcoinTransaction of which we want to calculate the hash
*
* @return byte array containing the hash of the transaction. Note: This one can be compared to a prevTransactionHash. However, if you want to search for it in popular blockchain explorers then you need to apply the function BitcoinUtil.reverseByteArray to it!
*
*
* @throws java.io.IOException in case of errors reading from the InputStream
*
*/

public static byte[] getTransactionHashSegwit(BitcoinTransaction transaction) throws IOException{
	// convert transaction to byte array
	ByteArrayOutputStream transactionBAOS = new ByteArrayOutputStream();

	byte[] version = reverseByteArray(convertIntToByteArray((int)transaction.getVersion()));
	transactionBAOS.write(version);
	// check if segwit
	boolean segwit=false;
	if ((transaction.getMarker()==0) && (transaction.getFlag()!=0)) {
		segwit=true;
		// we still need to check the case that all witness script stack items for all input transactions are of size 0 => traditional transaction hash calculation
		// cf. https://github.com/bitcoin/bips/blob/master/bip-0141.mediawiki
			// A non-witness program (defined hereinafter) txin MUST be associated with an empty witness field, represented by a 0x00. If all txins are not witness program, a transaction's wtxid is equal to its txid.
		boolean emptyWitness=true;
		for (int k=0;k<transaction.getBitcoinScriptWitness().size();k++) {
			BitcoinScriptWitnessItem currentItem = transaction.getBitcoinScriptWitness().get(k);
			if (currentItem.getStackItemCounter().length>1) {
				emptyWitness=false;
				break;
			} else if ((currentItem.getStackItemCounter().length==1) && (currentItem.getStackItemCounter()[0]!=0x00)) {
				emptyWitness=false;
				break;
			}
		}
		if (emptyWitness==true) {
			return BitcoinUtil.getTransactionHashSegwit(transaction);
		}
		transactionBAOS.write(transaction.getMarker());
		transactionBAOS.write(transaction.getFlag());
	}
	byte[] inCounter = transaction.getInCounter();
	transactionBAOS.write(inCounter);
	for (int i=0;i<transaction.getListOfInputs().size();i++) {
		transactionBAOS.write(transaction.getListOfInputs().get(i).getPrevTransactionHash());
		transactionBAOS.write(reverseByteArray(convertIntToByteArray((int)(transaction.getListOfInputs().get(i).getPreviousTxOutIndex()))));
		transactionBAOS.write(transaction.getListOfInputs().get(i).getTxInScriptLength());
		transactionBAOS.write(transaction.getListOfInputs().get(i).getTxInScript());
		transactionBAOS.write(reverseByteArray(convertIntToByteArray((int)(transaction.getListOfInputs().get(i).getSeqNo()))));
	}
	byte[] outCounter = transaction.getOutCounter();
	transactionBAOS.write(outCounter);
	for (int j=0;j<transaction.getListOfOutputs().size();j++) {
		transactionBAOS.write(convertBigIntegerToByteArray(transaction.getListOfOutputs().get(j).getValue(),8));
		transactionBAOS.write(transaction.getListOfOutputs().get(j).getTxOutScriptLength());
		transactionBAOS.write(transaction.getListOfOutputs().get(j).getTxOutScript());
	}
	if (segwit) {
		for (int k=0;k<transaction.getBitcoinScriptWitness().size();k++) {
			BitcoinScriptWitnessItem currentItem = transaction.getBitcoinScriptWitness().get(k);
			transactionBAOS.write(currentItem.getStackItemCounter());
			for (int l=0;l<currentItem.getScriptWitnessList().size();l++) {
				transactionBAOS.write(currentItem.getScriptWitnessList().get(l).getWitnessScriptLength());
				transactionBAOS.write(currentItem.getScriptWitnessList().get(l).getWitnessScript());
			}
		}
	}
	byte[] lockTime=reverseByteArray(convertIntToByteArray((int)transaction.getLockTime()));
	transactionBAOS.write(lockTime);
	byte[] transactionByteArray= transactionBAOS.toByteArray();
	byte[] firstRoundHash;
	byte[] secondRoundHash;
	try {
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		firstRoundHash = digest.digest(transactionByteArray);
		secondRoundHash = digest.digest(firstRoundHash);
	} catch (NoSuchAlgorithmException nsae) {
		LOG.error(nsae);
		return new byte[0];
	}
	return secondRoundHash;
}


}
