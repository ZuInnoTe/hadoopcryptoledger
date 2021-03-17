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
import java.util.List;


public class EthereumBlock implements Serializable {
private EthereumBlockHeader ethereumBlockHeader;
private List<EthereumTransaction> ethereumTransactions;
private List<EthereumBlockHeader> uncleHeaders;




public EthereumBlock() {

}

public EthereumBlock(EthereumBlockHeader ethereumBlockHeader,  List<EthereumTransaction> ethereumTransactions, List<EthereumBlockHeader> uncleHeaders) {
	this.ethereumBlockHeader=ethereumBlockHeader;
	this.ethereumTransactions=ethereumTransactions;
	this.uncleHeaders=uncleHeaders;
}

public EthereumBlockHeader getEthereumBlockHeader() {
	return ethereumBlockHeader;
}


public List<EthereumBlockHeader> getUncleHeaders() {
	return uncleHeaders;
}



public List<EthereumTransaction> getEthereumTransactions() {
	return ethereumTransactions;
}

public void set(EthereumBlock newBlock) {
	this.ethereumBlockHeader=newBlock.getEthereumBlockHeader();
	this.uncleHeaders=newBlock.getUncleHeaders();
	this.ethereumTransactions=newBlock.getEthereumTransactions();
}



}
