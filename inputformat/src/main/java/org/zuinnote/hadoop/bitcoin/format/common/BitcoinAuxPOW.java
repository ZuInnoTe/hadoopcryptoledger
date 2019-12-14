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
package org.zuinnote.hadoop.bitcoin.format.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class BitcoinAuxPOW implements Serializable {
	private int version;
	private BitcoinTransaction coinbaseTransaction;
	private byte[] parentBlockHeaderHash;
	private BitcoinAuxPOWBranch coinbaseBranch;
	private BitcoinAuxPOWBranch auxBlockChainBranch;
	private BitcoinAuxPOWBlockHeader parentBlockHeader;

	/*
	 * Creates an empy AuxPOW object in case the feature is not used (e.g. in the main Bitcoin blockchain)
	 * 
	 */
	public BitcoinAuxPOW() {
		this.version=0;
		this.coinbaseTransaction=null;
		this.coinbaseBranch=null;
		this.auxBlockChainBranch=null;
		this.parentBlockHeader=null;
	}
	
	public BitcoinAuxPOW(int version, BitcoinTransaction coinbaseTransaction, byte[] parentBlockHeaderHash, BitcoinAuxPOWBranch coinbaseBranch, BitcoinAuxPOWBranch auxBlockChainBranch, BitcoinAuxPOWBlockHeader parentBlockHeader) {
		this.version=version;
		this.coinbaseTransaction=coinbaseTransaction;
		this.parentBlockHeaderHash=parentBlockHeaderHash;
		this.coinbaseBranch=coinbaseBranch;
		this.auxBlockChainBranch=auxBlockChainBranch;
		this.parentBlockHeader=parentBlockHeader;
	}
	
	public int getVersion() {
		return version;
	}

	public BitcoinAuxPOWBranch getCoinbaseBranch() {
		return coinbaseBranch;
	}
	public void setCoinbaseBranch(BitcoinAuxPOWBranch coinbaseBranch) {
		this.coinbaseBranch = coinbaseBranch;
	}
	public BitcoinAuxPOWBranch getAuxBlockChainBranch() {
		return auxBlockChainBranch;
	}

	public BitcoinAuxPOWBlockHeader getParentBlockHeader() {
		return parentBlockHeader;
	}

	public BitcoinTransaction getCoinbaseTransaction() {
		return coinbaseTransaction;
	}

	public byte[] getParentBlockHeaderHash() {
		return parentBlockHeaderHash;
	}




}
