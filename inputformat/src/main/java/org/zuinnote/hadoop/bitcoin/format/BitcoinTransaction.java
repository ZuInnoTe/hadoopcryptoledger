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

import java.io.Serializable;


public class BitcoinTransaction implements Serializable {

/**
	 * 
	 */
	private static final long serialVersionUID = -5384663937499435810L;
	
private int version;
private byte[] inCounter;
private BitcoinTransactionInput[] listOfInputs;
private BitcoinTransactionOutput[] listOfOutputs;
private int lockTime;

public BitcoinTransaction(int version, byte[] inCounter, BitcoinTransactionInput[] listOfInputs, BitcoinTransactionOutput[] listOfOutputs, int lockTime) {
	this.version=version;
	this.inCounter=inCounter;
	this.listOfInputs=listOfInputs;
	this.listOfOutputs=listOfOutputs;
	this.lockTime=lockTime;
}

public int getVersion() {
	return this.version;
}

public byte[] getInCounter() {
	return this.inCounter;
}

public BitcoinTransactionInput[] getListOfInputs() {
	return this.listOfInputs;
}

public BitcoinTransactionOutput[] getListOfOutputs() {
	return this.listOfOutputs;
}

public int getLockTime() {
	return this.lockTime;
}

}
