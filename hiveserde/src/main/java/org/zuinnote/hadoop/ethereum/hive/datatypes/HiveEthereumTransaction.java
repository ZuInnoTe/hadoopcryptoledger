/**
* Copyright 2018 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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
package org.zuinnote.hadoop.ethereum.hive.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.io.Writable;

/**
 * 
 *
 */
public class HiveEthereumTransaction implements Writable {

private byte[] nonce;
private HiveDecimal value;
private byte[] valueRaw;
private byte[] receiveAddress;
private HiveDecimal gasPrice;
private byte[] gasPriceRaw;
private HiveDecimal gasLimit;
private byte[] gasLimitRaw;

private byte[] data;
private byte[] sig_v;
private byte[] sig_r;
private byte[] sig_s;

public HiveEthereumTransaction() {
	// please use setter to set the data
}

@Override
public void write(DataOutput out) throws IOException {
	   throw new UnsupportedOperationException("write unsupported");	
}

@Override
public void readFields(DataInput in) throws IOException {
	   throw new UnsupportedOperationException("readFields unsupported");
}

public byte[] getNonce() {
	return nonce;
}

public void setNonce(byte[] nonce) {
	this.nonce = nonce;
}

public HiveDecimal getValue() {
	return this.value;
}

public void setValue(HiveDecimal value) {
	this.value = value;
}

public byte[] getReceiveAddress() {
	return receiveAddress;
}

public void setReceiveAddress(byte[] receiveAddress) {
	this.receiveAddress = receiveAddress;
}


public HiveDecimal getGasPrice() {
	return this.gasPrice;
}

public void setGasPrice(HiveDecimal gasPrice) {
	this.gasPrice = gasPrice;
}

public HiveDecimal getGasLimit() {
	return this.gasLimit;
}

public void setGasLimit(HiveDecimal gasLimit) {
	this.gasLimit = gasLimit;
}

public byte[] getData() {
	return data;
}

public void setData(byte[] data) {
	this.data = data;
}



public void set(HiveEthereumTransaction newTransaction) {
	this.nonce=newTransaction.getNonce();
	this.valueRaw=newTransaction.getValueRaw();
	this.value=newTransaction.getValue();
	this.receiveAddress=newTransaction.getReceiveAddress();
	this.gasPriceRaw=newTransaction.getGasPriceRaw();
	this.gasPrice = newTransaction.getGasPrice();
	this.gasLimitRaw=newTransaction.getGasLimitRaw();
	this.gasLimit=newTransaction.getGasLimit();
	this.data=newTransaction.getData();
	this.sig_v=newTransaction.getSig_v();
	this.sig_r=newTransaction.getSig_r();
	this.sig_s=newTransaction.getSig_s();
}

public byte[] getSig_v() {
	return sig_v;
}

public void setSig_v(byte[] sig_v) {
	this.sig_v = sig_v;
}

public byte[] getSig_r() {
	return sig_r;
}

public void setSig_r(byte[] sig_r) {
	this.sig_r = sig_r;
}

public byte[] getSig_s() {
	return sig_s;
}

public void setSig_s(byte[] sig_s) {
	this.sig_s = sig_s;
}

public byte[] getValueRaw() {
	return valueRaw;
}

public void setValueRaw(byte[] valueRaw) {
	this.valueRaw = valueRaw;
}

public byte[] getGasPriceRaw() {
	return gasPriceRaw;
}

public void setGasPriceRaw(byte[] gasPriceRaw) {
	this.gasPriceRaw = gasPriceRaw;
}

public byte[] getGasLimitRaw() {
	return gasLimitRaw;
}

public void setGasLimitRaw(byte[] gasLimitRaw) {
	this.gasLimitRaw = gasLimitRaw;
}

}
