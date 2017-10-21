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

/**
 *
 *
 */
public class EthereumBlockHeader {
	private byte[] parentHash;
	private byte[] uncleHash;
	private byte[] coinBase;
	private byte[] stateRoot;
	private byte[] txTrieRoot;
	private byte[] receiptTrieRoot;
	private byte[] logsBloom;
	private byte[] difficulty;
	private long timestamp;
	private long number;
	private byte[] gasLimit;
	private long gasUsed;
	private byte[] mixHash;
	private byte[] extraData;
	private byte[] nonce;
	private byte[] hashCache;
}
