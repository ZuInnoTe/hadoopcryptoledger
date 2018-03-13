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

package org.zuinnote.hadoop.bitcoin.hive.udf;

import java.util.ArrayList;
import java.util.List;

import org.zuinnote.hadoop.bitcoin.format.common.BitcoinTransaction;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinTransactionOutput;
import org.zuinnote.hadoop.bitcoin.hive.datatypes.HiveBitcoinTransaction;
import org.zuinnote.hadoop.bitcoin.hive.datatypes.HiveBitcoinTransactionOutput;

/**
 * 
 *
 */
public class BitcoinUDFUtil {
	
	
	/***
	 * Convert HiveBitcoinTransaction to BitcoinTransaction (e.g. to get HashValue)
	 * 
	 * @param transaction HiveBitcoinTransaction
	 * @return transaction in BitcoinTransaction format
	 */
	public static BitcoinTransaction convertToBitcoinTransaction(HiveBitcoinTransaction transaction) {
		List<BitcoinTransactionOutput> newTransactionsOutputList = new ArrayList<>();
		for (int j = 0; j < transaction.getListOfOutputs().size(); j++) {
			HiveBitcoinTransactionOutput currentOutput = transaction.getListOfOutputs().get(j);
			newTransactionsOutputList.add(new BitcoinTransactionOutput(currentOutput.getValue().bigDecimalValue().toBigIntegerExact(),
					currentOutput.getTxOutScriptLength(), currentOutput.getTxOutScript()));
		}
		BitcoinTransaction result = new BitcoinTransaction(transaction.getMarker(),transaction.getFlag(),transaction.getVersion(), transaction.getInCounter(), transaction.getListOfInputs(),  transaction.getOutCounter(), newTransactionsOutputList, transaction.getBitcoinScriptWitness(), transaction.getLockTime());
		return result;
	}

}
