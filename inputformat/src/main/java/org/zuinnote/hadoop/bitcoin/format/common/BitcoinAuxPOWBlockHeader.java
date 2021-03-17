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

public class BitcoinAuxPOWBlockHeader implements Serializable {
	private long version;
	private byte[] previousBlockHash;
	private byte[] merkleRoot;
	private long time;
	private byte[] bits;
	private long nonce;


	public BitcoinAuxPOWBlockHeader(long version, byte[] previousBlockHash, byte[] merkleRoot, long time, byte[] bits, long nonce) {
		this.version=version;
		this.previousBlockHash=previousBlockHash;
		this.merkleRoot=merkleRoot;
		this.time=time;
		this.bits=bits;
		this.nonce=nonce;
	}

	public long getVersion() {
		return version;
	}

	public byte[] getPreviousBlockHash() {
		return previousBlockHash;
	}

	public byte[] getMerkleRoot() {
		return merkleRoot;
	}

	public long getTime() {
		return time;
	}

	public byte[] getBits() {
		return bits;
	}

	public long getNonce() {
		return nonce;
	}

}
