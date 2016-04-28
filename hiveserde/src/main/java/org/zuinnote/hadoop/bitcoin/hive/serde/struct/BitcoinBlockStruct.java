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

package org.zuinnote.hadoop.bitcoin.hive.serde.struct;

import java.util.List;



/**
* This class is an object representing a Bitcoin block as a struct in Hive. 
*/

public class BitcoinBlockStruct {
public byte[] magicNo;
public int blockSize;
public int version;
public int time;
public byte[] bits;
public int nonce;
public long transactionCounter;
public byte[] hashPrevBlock;
public byte[] hashMerkleRoot;
public List<BitcoinTransactionStruct> transactions;




}
