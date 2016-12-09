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

import java.io.Serializable;

public class BitcoinTransactionInput implements Serializable {

/**
	 * 
	 */
	private static final long serialVersionUID = 283893453089295979L;
	
private byte[] prevTransactionHash;
private long previousTxOutIndex;
private byte[] txInScriptLength;
private byte[] txInScript;
private long seqNo;

public BitcoinTransactionInput(byte[] prevTransactionHash, long previousTxOutIndex, byte[] txInScriptLength, byte[] txInScript, long seqNo) {
	this.prevTransactionHash=prevTransactionHash;
	this.previousTxOutIndex=previousTxOutIndex;
	this.txInScriptLength=txInScriptLength;
	this.txInScript=txInScript;
	this.seqNo=seqNo;
}

public byte[] getPrevTransactionHash() {
	return this.prevTransactionHash;
}

public long getPreviousTxOutIndex() {
	return this.previousTxOutIndex;
}

public byte[] getTxInScriptLength() {
	return this.txInScriptLength;
}

public byte[] getTxInScript() {
	return this.txInScript;
}

public long getSeqNo() {
	return this.seqNo;
}

}
