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

package org.zuinnote.hadoop.bitcoin.hive.udf;

import org.apache.hadoop.io.BytesWritable; 
import org.apache.hadoop.io.Text; 

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import org.zuinnote.hadoop.bitcoin.format.*;
import org.zuinnote.hadoop.bitcoin.hive.serde.struct.*;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;


/*
* Generic UDF to calculate the hash value of a transaction. It can be used to create a graph of transactions (cf. https://en.bitcoin.it/wiki/Transaction#general_format_.28inside_a_block.29_of_each_input_of_a_transaction_-_Txin)
*
*
*/

public class BitcoinTransactionHashUDF extends GenericUDF {


private static final Log LOG = LogFactory.getLog(BitcoinTransactionHashUDF.class.getName());

private StructObjectInspector oi;


@Override
public String getDisplayString(String[] arg0) {
	return("bitcoinTransactionHash()");
}

@Override
public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
  	if (arguments.length != 1)
      		throw new UDFArgumentLengthException("bitcoinTransactionHash only takes one argument: Struct<BitcoinTransaction> ");
	if (!(arguments[0] instanceof StructObjectInspector)) 
		throw new UDFArgumentException("first argument must be a Struct containing a BitcoinTransaction");
	oi = (StructObjectInspector)arguments[0];
	// the UDF returns the hash value of a BitcoinTransaction as byte array
	return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
}

@Override
public Object evaluate(DeferredObject[] arguments) throws HiveException {
	if (arguments==null) return null;
	if (!(arguments[0].get() instanceof BitcoinTransactionStruct)) return null;
	BitcoinTransactionStruct bitcoinTransactionStruct = (BitcoinTransactionStruct)arguments[0].get();
	// convert to BitcoinTransaction
	BitcoinTransactionInput[] bitcoinTransactionInputArray = new BitcoinTransactionInput[bitcoinTransactionStruct.listOfInputs.size()];
	for (int i=0;i<bitcoinTransactionInputArray.length;i++) {
		BitcoinTransactionInputStruct currentInputStruct = bitcoinTransactionStruct.listOfInputs.get(i);
		BitcoinTransactionInput currentInput = new BitcoinTransactionInput(currentInputStruct.prevTransactionHash,currentInputStruct.previousTxOutIndex,currentInputStruct.txInScriptLength,currentInputStruct.txInScript,currentInputStruct.seqNo);
		bitcoinTransactionInputArray[i]=currentInput;
	}
	BitcoinTransactionOutput[] bitcoinTransactionOutputArray = new BitcoinTransactionOutput[bitcoinTransactionStruct.listOfOutputs.size()];
	for (int i=0;i<bitcoinTransactionOutputArray.length;i++) {
		BitcoinTransactionOutputStruct currentOutputStruct = bitcoinTransactionStruct.listOfOutputs.get(i);
		BitcoinTransactionOutput currentOutput = new BitcoinTransactionOutput(currentOutputStruct.value,currentOutputStruct.txOutScriptLength,currentOutputStruct.txOutScript);
		bitcoinTransactionOutputArray[i]=currentOutput;
	}
	BitcoinTransaction bitcoinTransaction = new BitcoinTransaction(bitcoinTransactionStruct.version,bitcoinTransactionStruct.inCounter,bitcoinTransactionInputArray,bitcoinTransactionStruct.outCounter,bitcoinTransactionOutputArray,bitcoinTransactionStruct.lockTime);
	byte[] transactionHash=null;
	try {
		 transactionHash = BitcoinUtil.getTransactionHash(bitcoinTransaction);
	} catch (NoSuchAlgorithmException nsae) {
		throw new HiveException(nsae.toString());
	} catch (IOException ioe) {
		throw new HiveException(ioe.toString());
	}
	return new BytesWritable(transactionHash);
}

}
