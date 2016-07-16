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

public class TestBitcoinTransactionOutput implements Serializable {

/**
	 * 
	 */
	private static final long serialVersionUID = 2854570631540937753L;
	
private LongWritable value;
private BytesWritable txOutScriptLength;
private BytesWritable txOutScript;

public TestBitcoinTransactionOutput(long value, byte[] txOutScriptLength, byte[] txOutScript) {
	this.value=new LongWritable(value);
	this.txOutScriptLength=new BytesWritable(txOutScriptLength);
	this.txOutScript=new BytesWritable(txOutScript);
}

public LongWritable getValue() {
	return this.value;
}

public BytesWritable getTxOutScriptLength() {
	return this.txOutScriptLength;
}

public BytesWritable getTxOutScript() {
	return this.txOutScript;
}

}
