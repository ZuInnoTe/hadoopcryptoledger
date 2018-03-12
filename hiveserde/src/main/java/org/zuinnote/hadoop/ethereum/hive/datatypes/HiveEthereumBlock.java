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
import java.util.List;

import org.apache.hadoop.io.Writable;


public class HiveEthereumBlock implements Writable {
private HiveEthereumBlockHeader ethereumBlockHeader;
private List<HiveEthereumTransaction> ethereumTransactions;
private List<HiveEthereumBlockHeader> uncleHeaders;




public HiveEthereumBlock() {
	
}

public HiveEthereumBlock(HiveEthereumBlockHeader ethereumBlockHeader,  List<HiveEthereumTransaction> ethereumTransactions, List<HiveEthereumBlockHeader> uncleHeaders) {
	this.ethereumBlockHeader=ethereumBlockHeader;
	this.ethereumTransactions=ethereumTransactions;
	this.uncleHeaders=uncleHeaders;
}

public HiveEthereumBlockHeader getEthereumBlockHeader() {
	return ethereumBlockHeader;
}


public List<HiveEthereumBlockHeader> getUncleHeaders() {
	return uncleHeaders;
}



public List<HiveEthereumTransaction> getEthereumTransactions() {
	return ethereumTransactions;
}

public void set(HiveEthereumBlock newBlock) {
	this.ethereumBlockHeader=newBlock.getEthereumBlockHeader();
	this.uncleHeaders=newBlock.getUncleHeaders();
	this.ethereumTransactions=newBlock.getEthereumTransactions();
}

@Override
public void write(DataOutput out) throws IOException {
	   throw new UnsupportedOperationException("write unsupported");	
}

@Override
public void readFields(DataInput in) throws IOException {
	   throw new UnsupportedOperationException("readFields unsupported");
}


}
