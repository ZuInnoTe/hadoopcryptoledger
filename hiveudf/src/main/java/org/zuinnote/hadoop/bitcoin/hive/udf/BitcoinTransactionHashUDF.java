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


import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import org.zuinnote.hadoop.bitcoin.format.common.*;
import org.zuinnote.hadoop.bitcoin.hive.datatypes.HiveBitcoinTransaction;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;

/*
* Generic UDF to calculate the hash value of a transaction (txid). It can be used to create a graph of transactions (cf. https://en.bitcoin.it/wiki/Transaction#general_format_.28inside_a_block.29_of_each_input_of_a_transaction_-_Txin)
*
*
*/
@Description(
	name = "hclBitcoinTransactionHash",
	value = "_FUNC_(Struct<BitcoinTransaction>) - calculates the hash of a BitcoinTransaction (txid) and returns it as byte array",
	extended = "Example:\n" +
	"  > SELECT hclBitcoinTransactionHash(transactions[0]) FROM BitcoinBlockChain LIMIT 1;\n")
public class BitcoinTransactionHashUDF extends GenericUDF {


private static final Log LOG = LogFactory.getLog(BitcoinTransactionHashUDF.class.getName());

private StructObjectInspector soi;
private WritableBinaryObjectInspector wboi;
private WritableIntObjectInspector wioi;
private WritableLongObjectInspector wloi;

private HiveDecimalObjectInspector hdoi;

/**
* Returns the display representation of the function
*
* @param arg0 arguments
*
*/

@Override
public String getDisplayString(String[] arg0) {
	return "hclBitcoinTransactionHash()";
}

/**
*
* Initialize HiveUDF and create object inspectors. It requires that the argument length is = 1 and that the ObjectInspector of arguments[0] is of type StructObjectInspector
*
* @param arguments array of length 1 containing one StructObjectInspector
*
* @return ObjectInspector that is able to parse the result of the evaluate method of the UDF (BinaryWritable)
*
* @throws org.apache.hadoop.hive.ql.exec.UDFArgumentException in case the first argument is not of StructObjectInspector
* @throws org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException in case the number of arguments is != 1
*
*/

@Override
public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
	if (arguments==null) {
      		throw new UDFArgumentLengthException("bitcoinTransactionHash only takes one argument: Struct<BitcoinTransaction> ");
	}
  	if (arguments.length != 1) {
      		throw new UDFArgumentLengthException("bitcoinTransactionHash only takes one argument: Struct<BitcoinTransaction> ");
	}
	if (!(arguments[0] instanceof StructObjectInspector)) { 
		throw new UDFArgumentException("first argument must be a Struct containing a BitcoinTransaction");
	}
	this.soi = (StructObjectInspector)arguments[0];
	// these are only used for bitcointransaction structs exported to other formats, such as ORC
	this.wboi = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
	this.wioi = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
	this.wloi = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
	this.hdoi = PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector;
	// the UDF returns the hash value of a BitcoinTransaction as byte array
	return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
}

/**
* This method evaluates a given Object (of type BitcoinTransaction) or a struct which has all necessary fields corresponding to a BitcoinTransaction. The first case occurs, if the UDF evaluates data represented in a table provided by the HiveSerde as part of the hadoocryptoledger library. The second case occurs, if BitcoinTransaction data has been imported in a table in another format, such as ORC or Parquet.
* 
* @param arguments array of length 1 containing one object of type BitcoinTransaction or a Struct representing a BitcoinTransaction
*
* @return BytesWritable containing a byte array with the double hash of the BitcoinTransaction
*
* @throws org.apache.hadoop.hive.ql.metadata.HiveException in case an internal HiveError occurred
*/

@Override
public Object evaluate(DeferredObject[] arguments) throws HiveException {
	if ((arguments==null) || (arguments.length!=1)) { 
		return null;
	}
	BitcoinTransaction bitcoinTransaction;
	if (arguments[0].get() instanceof HiveBitcoinTransaction) { // this happens if the table is in the original file format
		 bitcoinTransaction = BitcoinUDFUtil.convertToBitcoinTransaction((HiveBitcoinTransaction)arguments[0].get());
	} else { // this happens if the table has been imported into a more optimized analytics format, such as ORC. However, usually we expect that the first case will be used mostly (the hash is generated during extraction from the input format)
		// check if all bitcointransaction fields are available <struct<version:int,incounter:binary,outcounter:binary,listofinputs:array<struct<prevtransactionhash:binary,previoustxoutindex:bigint,txinscriptlength:binary,txinscript:binary,seqno:bigint>>,listofoutputs:array<struct<value:bigint,txoutscriptlength:binary,txoutscript:binary>>,locktime:int>
		Object originalObject=arguments[0].get();
		StructField versionSF=soi.getStructFieldRef("version");
		StructField incounterSF=soi.getStructFieldRef("incounter");
		StructField outcounterSF=soi.getStructFieldRef("outcounter");
		StructField listofinputsSF=soi.getStructFieldRef("listofinputs");
		StructField listofoutputsSF=soi.getStructFieldRef("listofoutputs");
		StructField locktimeSF=soi.getStructFieldRef("locktime");
		boolean inputsNull =  (incounterSF==null) || (listofinputsSF==null);
		boolean outputsNull = (outcounterSF==null) || (listofoutputsSF==null);
		boolean otherAttributeNull = (versionSF==null) || (locktimeSF==null);
		if (inputsNull || outputsNull || otherAttributeNull) {
			LOG.warn("Structure does not correspond to BitcoinTransaction");
			return null;
		} 
		int version = wioi.get(soi.getStructFieldData(originalObject,versionSF));
		byte[] inCounter = wboi.getPrimitiveJavaObject(soi.getStructFieldData(originalObject,incounterSF));
		byte[] outCounter = wboi.getPrimitiveJavaObject(soi.getStructFieldData(originalObject,outcounterSF));
		int locktime = wioi.get(soi.getStructFieldData(originalObject,locktimeSF));
		Object listofinputsObject = soi.getStructFieldData(originalObject,listofinputsSF);
		ListObjectInspector loiInputs=(ListObjectInspector)listofinputsSF.getFieldObjectInspector();
		List<BitcoinTransactionInput> listOfInputsArray = readListOfInputsFromTable(loiInputs,listofinputsObject);
		
		Object listofoutputsObject = soi.getStructFieldData(originalObject,listofoutputsSF);
		ListObjectInspector loiOutputs=(ListObjectInspector)listofoutputsSF.getFieldObjectInspector();
		List<BitcoinTransactionOutput> listOfOutputsArray = readListOfOutputsFromTable(loiOutputs,listofoutputsObject);
		bitcoinTransaction = new BitcoinTransaction(version,inCounter,listOfInputsArray,outCounter,listOfOutputsArray,locktime);

	}
	byte[] transactionHash=null;
	try {
		 transactionHash = BitcoinUtil.getTransactionHash(bitcoinTransaction);
	}  catch (IOException ioe) {
		LOG.error(ioe);
		throw new HiveException(ioe.toString());
	}
	return new BytesWritable(transactionHash);
}

/**
* Read list of Bitcoin transaction inputs from a table in Hive in any format (e.g. ORC, Parquet)
*
* @param loi ObjectInspector for processing the Object containing a list
* @param listOfInputsObject object containing the list of inputs to a Bitcoin Transaction
*
* @return a list of BitcoinTransactionInputs 
*
*/

private List<BitcoinTransactionInput> readListOfInputsFromTable(ListObjectInspector loi, Object listOfInputsObject) {
int listLength=loi.getListLength(listOfInputsObject);
List<BitcoinTransactionInput> result = new ArrayList<>(listLength);
StructObjectInspector listOfInputsElementObjectInspector = (StructObjectInspector)loi.getListElementObjectInspector();
for (int i=0;i<listLength;i++) {
	Object currentlistofinputsObject = loi.getListElement(listOfInputsObject,i);
	StructField prevtransactionhashSF = listOfInputsElementObjectInspector.getStructFieldRef("prevtransactionhash");
	StructField previoustxoutindexSF = listOfInputsElementObjectInspector.getStructFieldRef("previoustxoutindex");
	StructField txinscriptlengthSF = listOfInputsElementObjectInspector.getStructFieldRef("txinscriptlength");
	StructField txinscriptSF = listOfInputsElementObjectInspector.getStructFieldRef("txinscript");
	StructField seqnoSF = listOfInputsElementObjectInspector.getStructFieldRef("seqno");
	boolean prevFieldsNull = (prevtransactionhashSF==null) || (previoustxoutindexSF==null);
	boolean inFieldsNull = (txinscriptlengthSF==null) || (txinscriptSF==null);
	boolean otherAttribNull = seqnoSF==null;
	if (prevFieldsNull || inFieldsNull  || otherAttribNull) {
		LOG.warn("Invalid BitcoinTransactionInput detected at position "+i);
		return new ArrayList<>();
	}
	byte[] currentPrevTransactionHash = wboi.getPrimitiveJavaObject(listOfInputsElementObjectInspector.getStructFieldData(currentlistofinputsObject,prevtransactionhashSF));
	long currentPreviousTxOutIndex = wloi.get(listOfInputsElementObjectInspector.getStructFieldData(currentlistofinputsObject,previoustxoutindexSF));
	byte[] currentTxInScriptLength= wboi.getPrimitiveJavaObject(listOfInputsElementObjectInspector.getStructFieldData(currentlistofinputsObject,txinscriptlengthSF));
	byte[] currentTxInScript= wboi.getPrimitiveJavaObject(listOfInputsElementObjectInspector.getStructFieldData(currentlistofinputsObject,txinscriptSF));
	long currentSeqNo = wloi.get(listOfInputsElementObjectInspector.getStructFieldData(currentlistofinputsObject,seqnoSF));
	BitcoinTransactionInput currentBitcoinTransactionInput = new BitcoinTransactionInput(currentPrevTransactionHash,currentPreviousTxOutIndex,currentTxInScriptLength,currentTxInScript,currentSeqNo);
	result.add(currentBitcoinTransactionInput);
}
return result;
}

/**
* Read list of Bitcoin transaction outputs from a table in Hive in any format (e.g. ORC, Parquet)
*
* @param loi ObjectInspector for processing the Object containing a list
* @param listOfOutputsObject object containing the list of outputs to a Bitcoin Transaction
*
* @return a list of BitcoinTransactionOutputs 
*
*/

private List<BitcoinTransactionOutput> readListOfOutputsFromTable(ListObjectInspector loi, Object listOfOutputsObject) {
int listLength=loi.getListLength(listOfOutputsObject);
List<BitcoinTransactionOutput> result=new ArrayList<>(listLength);
StructObjectInspector listOfOutputsElementObjectInspector = (StructObjectInspector)loi.getListElementObjectInspector();
	for (int i=0;i<listLength;i++) {
		Object currentListOfOutputsObject = loi.getListElement(listOfOutputsObject,i);
		StructField valueSF = listOfOutputsElementObjectInspector.getStructFieldRef("value");
		StructField txoutscriptlengthSF = listOfOutputsElementObjectInspector.getStructFieldRef("txoutscriptlength");
		StructField txoutscriptSF = listOfOutputsElementObjectInspector.getStructFieldRef("txoutscript");
		if ((valueSF==null) || (txoutscriptlengthSF==null) || (txoutscriptSF==null)) {
			LOG.warn("Invalid BitcoinTransactionOutput detected at position "+i);
			return new ArrayList<>();
		}
		HiveDecimal currentValue=hdoi.getPrimitiveJavaObject(listOfOutputsElementObjectInspector.getStructFieldData(currentListOfOutputsObject,valueSF));	
		byte[] currentTxOutScriptLength=wboi.getPrimitiveJavaObject(listOfOutputsElementObjectInspector.getStructFieldData(currentListOfOutputsObject,txoutscriptlengthSF));
		byte[] currentTxOutScript=wboi.getPrimitiveJavaObject(listOfOutputsElementObjectInspector.getStructFieldData(currentListOfOutputsObject,txoutscriptSF));
		BitcoinTransactionOutput currentBitcoinTransactionOutput = new BitcoinTransactionOutput(currentValue.bigDecimalValue().toBigIntegerExact(),currentTxOutScriptLength,currentTxOutScript);
		result.add(currentBitcoinTransactionOutput);
	}
return result;
}



}
