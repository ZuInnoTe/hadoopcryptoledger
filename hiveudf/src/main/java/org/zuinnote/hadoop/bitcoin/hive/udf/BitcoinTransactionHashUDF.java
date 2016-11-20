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
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import org.zuinnote.hadoop.bitcoin.format.*;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import java.util.List;
import java.util.ArrayList;

/*
* Generic UDF to calculate the hash value of a transaction. It can be used to create a graph of transactions (cf. https://en.bitcoin.it/wiki/Transaction#general_format_.28inside_a_block.29_of_each_input_of_a_transaction_-_Txin)
*
* CREATE TEMPORARY FUNCTION hclBitcoinTransactionHash as 'org.zuinnote.hadoop.bitcoin.hive.udf.BitcoinTransactionHashUDF';
*
*/
@Description(
	name = "hclBitcoinTransactionHash",
	value = "_FUNC_(Struct<BitcoinTransaction>) - calculates the hash of a BitcoinTransaction and returns it as byte array",
	extended = "Example:\n" +
	"  > SELECT hclBitcoinTransactionHash(transactions[0]) FROM BitcoinBlockChain LIMIT 1;\n")
public class BitcoinTransactionHashUDF extends GenericUDF {


private static final Log LOG = LogFactory.getLog(BitcoinTransactionHashUDF.class.getName());

private StructObjectInspector soi;
private WritableBinaryObjectInspector wboi;
private WritableIntObjectInspector wioi;
private WritableLongObjectInspector wloi;


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
	if (arguments==null) 
      		throw new UDFArgumentLengthException("bitcoinTransactionHash only takes one argument: Struct<BitcoinTransaction> ");
  	if (arguments.length != 1)
      		throw new UDFArgumentLengthException("bitcoinTransactionHash only takes one argument: Struct<BitcoinTransaction> ");
	if (!(arguments[0] instanceof StructObjectInspector)) 
		throw new UDFArgumentException("first argument must be a Struct containing a BitcoinTransaction");
	this.soi = (StructObjectInspector)arguments[0];
	// these are only used for bitcointransaction structs exported to other formats, such as ORC
	this.wboi = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
	this.wioi = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
	this.wloi = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
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
* @throws org.apache.hadoop.hive.ql.metadata.HiveException in case an itnernal HiveError occurred
*/

@Override
public Object evaluate(DeferredObject[] arguments) throws HiveException {
	if ((arguments==null) || (arguments.length!=1)) { 
		return null;
	}
	BitcoinTransaction bitcoinTransaction=null;
	if (arguments[0].get() instanceof BitcoinTransaction) { // this happens if the table is in the original file format
		 bitcoinTransaction = (BitcoinTransaction)arguments[0].get();
	} else { // this happens if the table has been imported into a more optimized analytics format, such as ORC. However, usually we expect that the first case will be used mostly (the hash is generated during extraction from the input format)
		// check if all bitcointransaction fields are available <struct<version:int,incounter:binary,outcounter:binary,listofinputs:array<struct<prevtransactionhash:binary,previoustxoutindex:bigint,txinscriptlength:binary,txinscript:binary,seqno:bigint>>,listofoutputs:array<struct<value:bigint,txoutscriptlength:binary,txoutscript:binary>>,locktime:int>
		Object originalObject=arguments[0].get();
		StructField versionSF=soi.getStructFieldRef("version");
		StructField incounterSF=soi.getStructFieldRef("incounter");
		StructField outcounterSF=soi.getStructFieldRef("outcounter");
		StructField listofinputsSF=soi.getStructFieldRef("listofinputs");
		StructField listofoutputsSF=soi.getStructFieldRef("listofoutputs");
		StructField locktimeSF=soi.getStructFieldRef("locktime");
		if ((versionSF==null) || (incounterSF==null) || (outcounterSF==null) || (listofinputsSF==null) || (listofoutputsSF==null) || (locktimeSF==null)) {
			LOG.info("Structure does not correspond to BitcoinTransaction");
			return null;
		} 
		int version = wioi.get(soi.getStructFieldData(originalObject,versionSF));
		byte[] inCounter = wboi.getPrimitiveJavaObject(soi.getStructFieldData(originalObject,incounterSF));
		byte[] outCounter = wboi.getPrimitiveJavaObject(soi.getStructFieldData(originalObject,outcounterSF));
		int locktime = wioi.get(soi.getStructFieldData(originalObject,locktimeSF));
		Object listofinputsObject = soi.getStructFieldData(originalObject,listofinputsSF);
		ListObjectInspector loiInputs=(ListObjectInspector)listofinputsSF.getFieldObjectInspector();
		int loiInputsLength=loiInputs.getListLength(listofinputsObject);
		StructObjectInspector listofinputsElementObjectInspector = (StructObjectInspector)loiInputs.getListElementObjectInspector();
		List<BitcoinTransactionInput> listOfInputsArray = new ArrayList<BitcoinTransactionInput>(loiInputsLength);
		for (int i=0;i<loiInputsLength;i++) {
			Object currentlistofinputsObject = loiInputs.getListElement(listofinputsObject,i);
			StructField prevtransactionhashSF = listofinputsElementObjectInspector.getStructFieldRef("prevtransactionhash");
			StructField previoustxoutindexSF = listofinputsElementObjectInspector.getStructFieldRef("previoustxoutindex");
			StructField txinscriptlengthSF = listofinputsElementObjectInspector.getStructFieldRef("txinscriptlength");
			StructField txinscriptSF = listofinputsElementObjectInspector.getStructFieldRef("txinscript");
			StructField seqnoSF = listofinputsElementObjectInspector.getStructFieldRef("seqno");
			if ((prevtransactionhashSF==null) || (previoustxoutindexSF==null) || (txinscriptlengthSF==null) || (txinscriptSF==null) || (seqnoSF==null)) {
				LOG.info("Invalid BitcoinTransactionInput detected at position "+i);
				return null;
			}
			byte[] currentPrevTransactionHash = wboi.getPrimitiveJavaObject(listofinputsElementObjectInspector.getStructFieldData(currentlistofinputsObject,prevtransactionhashSF));
			long currentPreviousTxOutIndex = wloi.get(listofinputsElementObjectInspector.getStructFieldData(currentlistofinputsObject,previoustxoutindexSF));
			byte[] currentTxInScriptLength= wboi.getPrimitiveJavaObject(listofinputsElementObjectInspector.getStructFieldData(currentlistofinputsObject,txinscriptlengthSF));
			byte[] currentTxInScript= wboi.getPrimitiveJavaObject(listofinputsElementObjectInspector.getStructFieldData(currentlistofinputsObject,txinscriptSF));
			long currentSeqNo = wloi.get(listofinputsElementObjectInspector.getStructFieldData(currentlistofinputsObject,seqnoSF));
			BitcoinTransactionInput currentBitcoinTransactionInput = new BitcoinTransactionInput(currentPrevTransactionHash,currentPreviousTxOutIndex,currentTxInScriptLength,currentTxInScript,currentSeqNo);
			listOfInputsArray.add(currentBitcoinTransactionInput);
			
		}
		Object listofoutputsObject = soi.getStructFieldData(originalObject,listofoutputsSF);
		ListObjectInspector loiOutputs=(ListObjectInspector)listofoutputsSF.getFieldObjectInspector();
		StructObjectInspector listofoutputsElementObjectInspector = (StructObjectInspector)loiOutputs.getListElementObjectInspector();
		int loiOutputsLength=loiInputs.getListLength(listofinputsObject);
		List<BitcoinTransactionOutput> listOfOutputsArray = new ArrayList<BitcoinTransactionOutput>(loiOutputsLength);
		for (int i=0;i<loiOutputsLength;i++) {
			Object currentlistofoutputsObject = loiOutputs.getListElement(listofoutputsObject,i);
			StructField valueSF = listofoutputsElementObjectInspector.getStructFieldRef("value");
			StructField txoutscriptlengthSF = listofoutputsElementObjectInspector.getStructFieldRef("txoutscriptlength");
			StructField txoutscriptSF = listofoutputsElementObjectInspector.getStructFieldRef("txoutscript");
			if ((valueSF==null) || (txoutscriptlengthSF==null) || (txoutscriptSF==null)) {
				LOG.info("Invalid BitcoinTransactionOutput detected at position "+i);
				return null;
			}
			long currentValue=wloi.get(listofoutputsElementObjectInspector.getStructFieldData(currentlistofoutputsObject,valueSF));	
			byte[] currentTxOutScriptLength=wboi.getPrimitiveJavaObject(listofoutputsElementObjectInspector.getStructFieldData(currentlistofoutputsObject,txoutscriptlengthSF));
			byte[] currentTxOutScript=wboi.getPrimitiveJavaObject(listofoutputsElementObjectInspector.getStructFieldData(currentlistofoutputsObject,txoutscriptSF));
			BitcoinTransactionOutput currentBitcoinTransactionOutput = new BitcoinTransactionOutput(currentValue,currentTxOutScriptLength,currentTxOutScript);
			listOfOutputsArray.add(currentBitcoinTransactionOutput);
		}
		bitcoinTransaction = new BitcoinTransaction(version,inCounter,listOfInputsArray,outCounter,listOfOutputsArray,locktime);

	}
	byte[] transactionHash=null;
	try {
		 transactionHash = BitcoinUtil.getTransactionHash(bitcoinTransaction);
	} catch (NoSuchAlgorithmException nsae) {
		LOG.error(nsae);
		throw new HiveException(nsae.toString());
	} catch (IOException ioe) {
		LOG.error(ioe);
		throw new HiveException(ioe.toString());
	}
	return new BytesWritable(transactionHash);
}

}
