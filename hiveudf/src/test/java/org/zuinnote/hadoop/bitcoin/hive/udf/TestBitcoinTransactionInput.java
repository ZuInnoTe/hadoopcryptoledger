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

package org.zuinnote.hadoop.bitcoin.hive.udf;

import java.io.Serializable;

import org.apache.hadoop.io.*;

public class TestBitcoinTransactionInput implements Serializable {

/**
	 * 
	 */
	private static final long serialVersionUID = 283893453189295979L;
	
private BytesWritable prevTransactionHash;
private LongWritable previousTxOutIndex;
private BytesWritable txInScriptLength;
private BytesWritable txInScript;
private LongWritable seqNo;

public TestBitcoinTransactionInput(byte[] prevTransactionHash, long previousTxOutIndex, byte[] txInScriptLength, byte[] txInScript, long seqNo) {
	this.prevTransactionHash=new BytesWritable(prevTransactionHash);
	this.previousTxOutIndex=new LongWritable(previousTxOutIndex);
	this.txInScriptLength=new BytesWritable(txInScriptLength);
	this.txInScript=new BytesWritable(txInScript);
	this.seqNo=new LongWritable(seqNo);
}

public BytesWritable getPrevTransactionHash() {
	return this.prevTransactionHash;
}

public LongWritable getPreviousTxOutIndex() {
	return this.previousTxOutIndex;
}

public BytesWritable getTxInScriptLength() {
	return this.txInScriptLength;
}

public BytesWritable getTxInScript() {
	return this.txInScript;
}

public LongWritable getSeqNo() {
	return this.seqNo;
}

}
