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

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;


/**
 *
 *
 */
public class EthereumTransaction implements Serializable {

private byte[] nonce;
private BigInteger value;
private byte[] valueRaw;
private byte[] receiveAddress;
private BigInteger gasPrice;
private byte[] gasPriceRaw;
private BigInteger gasLimit;
private byte[] gasLimitRaw;

private byte[] data;
private byte[] sig_v;
private byte[] sig_r;
private byte[] sig_s;

public EthereumTransaction() {
	// please use setter to set the data
}



public byte[] getNonce() {
	return nonce;
}

public void setNonce(byte[] nonce) {
	this.nonce = nonce;
}

public BigInteger getValue() {
	if (value==null) {
		this.value=EthereumUtil.convertVarNumberToBigInteger(this.valueRaw);
	}
	return value;
}

public void setValue(BigInteger value) {
	this.value = value;
}

public byte[] getReceiveAddress() {
	return receiveAddress;
}

public void setReceiveAddress(byte[] receiveAddress) {
	this.receiveAddress = receiveAddress;
}


public BigInteger getGasPrice() {
	if (gasPrice==null) {
		this.gasPrice=EthereumUtil.convertVarNumberToBigInteger(this.gasPriceRaw);
	}
	return gasPrice;
}

public void setGasPrice(BigInteger gasPrice) {
	this.gasPrice = gasPrice;
}

public BigInteger getGasLimit() {
	if (gasLimit==null) {
		this.gasLimit=EthereumUtil.convertVarNumberToBigInteger(this.gasLimitRaw);
	}
	return gasLimit;
}

public void setGasLimit(BigInteger gasLimit) {
	this.gasLimit = gasLimit;
}

public byte[] getData() {
	return data;
}

public void setData(byte[] data) {
	this.data = data;
}



public void set(EthereumTransaction newTransaction) {
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
