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

import java.util.Arrays;




public class BitcoinScriptPatternParser {


private BitcoinScriptPatternParser() {
}

/**
* Get the payment destination from an scriptPubKey (output script of a transaction). This is based on standard scripts accepted by the Bitcoin network (https://en.bitcoin.it/wiki/Script).
*
* @param scriptPubKey output script of a transaction
*
* @return a string in the following format in case of (1) a standard transfer to a Bitcoin address : "bitcoinaddress_ADRESS" where ADDRESS is the Bitcoin address, (2) an (obsolete) transfer to a public key: "bitcoinpupkey_PUBKEY" where PUBKEY is the public key, (3) in case of output that cannot be spent: "unspendable", (4) in case anyone can spend: "anyone", (5) in case of transaction puzzle: "puzzle_HASH256" where HASH256 is the puzzle (6) in all other cases null (different type of Bitcoin transaction)
**/

public static String getPaymentDestination(byte[] scriptPubKey) {
	if (scriptPubKey==null) {
		return null;
	}
	// test if anyone can spend output
	if (scriptPubKey.length==0) {
		return "anyone"; // need to check also ScriptSig for OP_TRUE
	}
	// test if standard transaction to Bitcoin address
	String payToHash = checkPayToHash(scriptPubKey);
	if (payToHash!=null) {
		return payToHash;
	}
	// test if obsolete transaction to public key
	String payToPubKey = checkPayToPubKey(scriptPubKey);
	if (payToPubKey!=null) {
		return payToPubKey;
	}	
	// test if puzzle
	if ((scriptPubKey.length>0) && ((scriptPubKey[0] & 0xFF)==0xAA) && ((scriptPubKey[scriptPubKey.length-1] & 0xFF)==0x87)) {
		byte[] puzzle = Arrays.copyOfRange(scriptPubKey, 1, scriptPubKey.length-2);
		return "puzzle_"+BitcoinUtil.convertByteArrayToHexString(puzzle);
	}
	// test if unspendable
	if ((scriptPubKey.length>0) && ((scriptPubKey[0] & 0xFF)==0x6a)) {
		 return "unspendable";
	}
	return null;
}


/***
* Checks if scriptPubKey is about a transaction for paying to a hash
*
* @param scriptPubKey of transaction
*
* @return null, if transaction not about paying to hash, a string starting with "bitcoinaddress_" and ending with the hex values as String of the hash address
*
*/

private static String checkPayToHash(byte[] scriptPubKey) {
// test start
boolean validLength=scriptPubKey.length==25;
if (!(validLength)) {
	return null;
}
boolean validStart=((scriptPubKey[0] & 0xFF)==0x76) && ((scriptPubKey[1] & 0xFF)==0xA9) && ((scriptPubKey[2] & 0xFF)==0x14);
boolean validEnd=((scriptPubKey[23] & 0xFF)==0x88) && ((scriptPubKey[24]  & 0xFF)==0xAC);

	if (validStart && validEnd) {
		byte[] bitcoinAddress = Arrays.copyOfRange(scriptPubKey, 3, 23);
		return "bitcoinaddress_"+BitcoinUtil.convertByteArrayToHexString(bitcoinAddress);
	} 
	return null;
}

/***
* Checks if scriptPubKey is about a transaction for paying to a public key
*
* @param scriptPubKey of transaction
*
* @return null, if transaction not about paying to hash, a string starting with "bitcoinpubkey_" and ending with the hex values as String of the public key
*
*/
private static String checkPayToPubKey(byte[] scriptPubKey) {
if ((scriptPubKey.length>0) && ((scriptPubKey[scriptPubKey.length-1] & 0xFF)==0xAC)) {
		byte[] publicKey =Arrays.copyOfRange(scriptPubKey, 0, scriptPubKey.length-1);
		return "bitcoinpubkey_"+BitcoinUtil.convertByteArrayToHexString(publicKey);
	}
	return null;
}

}



