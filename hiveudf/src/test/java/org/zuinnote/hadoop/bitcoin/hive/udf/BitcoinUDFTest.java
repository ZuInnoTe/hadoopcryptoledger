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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.io.BytesWritable; 
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import  org.apache.hadoop.hive.ql.udf.generic.*;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;

import org.zuinnote.hadoop.bitcoin.format.common.*;

import java.util.ArrayList;
import java.util.List;



public class BitcoinUDFTest {


@Test
  public void BitcoinScriptPaymentPatternAnalyzerUDFNotNull() {
	BitcoinScriptPaymentPatternAnalyzerUDF bsppaUDF = new BitcoinScriptPaymentPatternAnalyzerUDF();
	byte[] txOutScriptGenesis= new byte[]{(byte)0x41,(byte)0x04,(byte)0x67,(byte)0x8A,(byte)0xFD,(byte)0xB0,(byte)0xFE,(byte)0x55,(byte)0x48,(byte)0x27,(byte)0x19,(byte)0x67,(byte)0xF1,(byte)0xA6,(byte)0x71,(byte)0x30,(byte)0xB7,(byte)0x10,(byte)0x5C,(byte)0xD6,(byte)0xA8,(byte)0x28,(byte)0xE0,(byte)0x39,(byte)0x09,(byte)0xA6,(byte)0x79,(byte)0x62,(byte)0xE0,(byte)0xEA,(byte)0x1F,(byte)0x61,(byte)0xDE,(byte)0xB6,(byte)0x49,(byte)0xF6,(byte)0xBC,(byte)0x3F,(byte)0x4C,(byte)0xEF,(byte)0x38,(byte)0xC4,(byte)0xF3,(byte)0x55,(byte)0x04,(byte)0xE5,(byte)0x1E,(byte)0xC1,(byte)0x12,(byte)0xDE,(byte)0x5C,(byte)0x38,(byte)0x4D,(byte)0xF7,(byte)0xBA,(byte)0x0B,(byte)0x8D,(byte)0x57,(byte)0x8A,(byte)0x4C,(byte)0x70,(byte)0x2B,(byte)0x6B,(byte)0xF1,(byte)0x1D,(byte)0x5F,(byte)0xAC};
       BytesWritable evalObj = new BytesWritable(txOutScriptGenesis);
	String result = bsppaUDF.evaluate(evalObj).toString();
	String comparatorText = "bitcoinpubkey_4104678AFDB0FE5548271967F1A67130B7105CD6A828E03909A67962E0EA1F61DEB649F6BC3F4CEF38C4F35504E51EC112DE5C384DF7BA0B8D578A4C702B6BF11D5F";
	assertEquals(comparatorText,result,"TxOutScript from Genesis should be payment to a pubkey address");
  }

@Test
  public void BitcoinScriptPaymentPatternAnalyzerUDFPaymentDestinationNull() {
	BitcoinScriptPaymentPatternAnalyzerUDF bsppaUDF = new BitcoinScriptPaymentPatternAnalyzerUDF();
	byte[] txOutScriptTestNull= new byte[]{(byte)0x00};
       BytesWritable evalObj = new BytesWritable(txOutScriptTestNull);
	Text result = bsppaUDF.evaluate(evalObj);
	assertNull( result,"Invalid payment script should be null for payment destination");
  }


@Test
  public void BitcoinScriptPaymentPatternAnalyzerUDFNull() {
	BitcoinScriptPaymentPatternAnalyzerUDF bsppaUDF = new BitcoinScriptPaymentPatternAnalyzerUDF();
	assertNull( bsppaUDF.evaluate(null),"Null argument to UDF returns null");
  }

 
@Test
  public void BitcoinTransactionHashUDFInvalidArguments() throws HiveException {
	final BitcoinTransactionHashUDF bthUDF = new BitcoinTransactionHashUDF();
	UDFArgumentLengthException exNull = assertThrows(UDFArgumentLengthException.class, ()->bthUDF.initialize(null), "Exception is thrown in case of null parameter");
	UDFArgumentLengthException exLen = assertThrows(UDFArgumentLengthException.class, ()->bthUDF.initialize(new ObjectInspector[2]), "Exception is thrown in case of invalid length parameter");
	
	StringObjectInspector[] testStringOI = new StringObjectInspector[1];
	testStringOI[0]=PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	UDFArgumentException wrongType = assertThrows(UDFArgumentException.class, ()->bthUDF.initialize(testStringOI), "Exception is thrown in case of invalid type of parameter");
	
  }

@Test
  public void BitcoinTransactionHashUDFNull() throws HiveException {
	BitcoinTransactionHashUDF bthUDF = new BitcoinTransactionHashUDF();
	ObjectInspector[] arguments = new ObjectInspector[1];
	arguments[0] =  ObjectInspectorFactory.getReflectionObjectInspector(BitcoinBlock.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
	bthUDF.initialize(arguments);	
	assertNull(bthUDF.evaluate(null),"Null argument to UDF returns null");
  }


@Test
  public void BitcoinTransactionHashUDFWriteable()  throws HiveException  {
	BitcoinTransactionHashUDF bthUDF = new BitcoinTransactionHashUDF();
	ObjectInspector[] arguments = new ObjectInspector[1];
	arguments[0] =  ObjectInspectorFactory.getReflectionObjectInspector(BitcoinTransaction.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
	bthUDF.initialize(arguments);	
// create BitcoinTransaction
 // reconstruct the transaction from the genesis block
	int version=1;
	byte[] inCounter = new byte[]{0x01};
	byte[] previousTransactionHash = new byte[]{0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
	long previousTxOutIndex = 4294967295L;
	byte[] txInScriptLength = new byte[]{(byte)0x4D};
	byte[] txInScript= new byte[]{(byte)0x04,(byte)0xFF,(byte)0xFF,(byte)0x00,(byte)0x1D,(byte)0x01,(byte)0x04,(byte)0x45,(byte)0x54,(byte)0x68,(byte)0x65,(byte)0x20,(byte)0x54,(byte)0x69,(byte)0x6D,(byte)0x65,(byte)0x73,(byte)0x20,(byte)0x30,(byte)0x33,(byte)0x2F,(byte)0x4A,(byte)0x61,(byte)0x6E,(byte)0x2F,(byte)0x32,(byte)0x30,(byte)0x30,(byte)0x39,(byte)0x20,(byte)0x43,(byte)0x68,(byte)0x61,(byte)0x6E,(byte)0x63,(byte)0x65,(byte)0x6C,(byte)0x6C,(byte)0x6F,(byte)0x72,(byte)0x20,(byte)0x6F,(byte)0x6E,(byte)0x20,(byte)0x62,(byte)0x72,(byte)0x69,(byte)0x6E,(byte)0x6B,(byte)0x20,(byte)0x6F,(byte)0x66,(byte)0x20,(byte)0x73,(byte)0x65,(byte)0x63,(byte)0x6F,(byte)0x6E,(byte)0x64,(byte)0x20,(byte)0x62,(byte)0x61,(byte)0x69,(byte)0x6C,(byte)0x6F,(byte)0x75,(byte)0x74,(byte)0x20,(byte)0x66,(byte)0x6F,(byte)0x72,(byte)0x20,(byte)0x62,(byte)0x61,(byte)0x6E,(byte)0x6B,(byte)0x73};
	long seqNo=4294967295L;
	byte[] outCounter = new byte[]{0x01};
	long value=5000000000L;
	byte[] txOutScriptLength=new byte[]{(byte)0x43};
	byte[] txOutScript=new byte[]{(byte)0x41,(byte)0x04,(byte)0x67,(byte)0x8A,(byte)0xFD,(byte)0xB0,(byte)0xFE,(byte)0x55,(byte)0x48,(byte)0x27,(byte)0x19,(byte)0x67,(byte)0xF1,(byte)0xA6,(byte)0x71,(byte)0x30,(byte)0xB7,(byte)0x10,(byte)0x5C,(byte)0xD6,(byte)0xA8,(byte)0x28,(byte)0xE0,(byte)0x39,(byte)0x09,(byte)0xA6,(byte)0x79,(byte)0x62,(byte)0xE0,(byte)0xEA,(byte)0x1F,(byte)0x61,(byte)0xDE,(byte)0xB6,(byte)0x49,(byte)0xF6,(byte)0xBC,(byte)0x3F,(byte)0x4C,(byte)0xEF,(byte)0x38,(byte)0xC4,(byte)0xF3,(byte)0x55,(byte)0x04,(byte)0xE5,(byte)0x1E,(byte)0xC1,(byte)0x12,(byte)0xDE,(byte)0x5C,(byte)0x38,(byte)0x4D,(byte)0xF7,(byte)0xBA,(byte)0x0B,(byte)0x8D,(byte)0x57,(byte)0x8A,(byte)0x4C,(byte)0x70,(byte)0x2B,(byte)0x6B,(byte)0xF1,(byte)0x1D,(byte)0x5F,(byte)0xAC};
	int lockTime = 0;
	List<BitcoinTransactionInput> genesisInput = new ArrayList<BitcoinTransactionInput>(1);
	genesisInput.add(new BitcoinTransactionInput(previousTransactionHash,previousTxOutIndex,txInScriptLength,txInScript,seqNo));
	List<BitcoinTransactionOutput> genesisOutput = new ArrayList<BitcoinTransactionOutput>(1);
	genesisOutput.add(new BitcoinTransactionOutput(value,txOutScriptLength,txOutScript));
	 BitcoinTransaction genesisTransaction = new BitcoinTransaction(version,inCounter,genesisInput,outCounter,genesisOutput,lockTime);
	 byte[] expectedHash = BitcoinUtil.reverseByteArray(new byte[]{(byte)0x4A,(byte)0x5E,(byte)0x1E,(byte)0x4B,(byte)0xAA,(byte)0xB8,(byte)0x9F,(byte)0x3A,(byte)0x32,(byte)0x51,(byte)0x8A,(byte)0x88,(byte)0xC3,(byte)0x1B,(byte)0xC8,(byte)0x7F,(byte)0x61,(byte)0x8F,(byte)0x76,(byte)0x67,(byte)0x3E,(byte)0x2C,(byte)0xC7,(byte)0x7A,(byte)0xB2,(byte)0x12,(byte)0x7B,(byte)0x7A,(byte)0xFD,(byte)0xED,(byte)0xA3,(byte)0x3B});
	GenericUDF.DeferredObject[] doa = new GenericUDF.DeferredObject[1];
	doa[0]=new GenericUDF.DeferredJavaObject(genesisTransaction);
	BytesWritable bw = (BytesWritable) bthUDF.evaluate(doa);
	
	assertArrayEquals( expectedHash,bw.copyBytes(),"BitcoinTransaction object genesis transaction hash from UDF");
  }


@Test
  public void BitcoinTransactionHashUDFObjectInspector() throws HiveException {
	BitcoinTransactionHashUDF bthUDF = new BitcoinTransactionHashUDF();
	ObjectInspector[] arguments = new ObjectInspector[1];
	arguments[0] =  ObjectInspectorFactory.getReflectionObjectInspector(TestBitcoinTransaction.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
	bthUDF.initialize(arguments);	
// create BitcoinTransaction
 // reconstruct the transaction from the genesis block
	int version=1;
	byte[] inCounter = new byte[]{0x01};
	byte[] previousTransactionHash = new byte[]{0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
	long previousTxOutIndex = 4294967295L;
	byte[] txInScriptLength = new byte[]{(byte)0x4D};
	byte[] txInScript= new byte[]{(byte)0x04,(byte)0xFF,(byte)0xFF,(byte)0x00,(byte)0x1D,(byte)0x01,(byte)0x04,(byte)0x45,(byte)0x54,(byte)0x68,(byte)0x65,(byte)0x20,(byte)0x54,(byte)0x69,(byte)0x6D,(byte)0x65,(byte)0x73,(byte)0x20,(byte)0x30,(byte)0x33,(byte)0x2F,(byte)0x4A,(byte)0x61,(byte)0x6E,(byte)0x2F,(byte)0x32,(byte)0x30,(byte)0x30,(byte)0x39,(byte)0x20,(byte)0x43,(byte)0x68,(byte)0x61,(byte)0x6E,(byte)0x63,(byte)0x65,(byte)0x6C,(byte)0x6C,(byte)0x6F,(byte)0x72,(byte)0x20,(byte)0x6F,(byte)0x6E,(byte)0x20,(byte)0x62,(byte)0x72,(byte)0x69,(byte)0x6E,(byte)0x6B,(byte)0x20,(byte)0x6F,(byte)0x66,(byte)0x20,(byte)0x73,(byte)0x65,(byte)0x63,(byte)0x6F,(byte)0x6E,(byte)0x64,(byte)0x20,(byte)0x62,(byte)0x61,(byte)0x69,(byte)0x6C,(byte)0x6F,(byte)0x75,(byte)0x74,(byte)0x20,(byte)0x66,(byte)0x6F,(byte)0x72,(byte)0x20,(byte)0x62,(byte)0x61,(byte)0x6E,(byte)0x6B,(byte)0x73};
	long seqNo=4294967295L;
	byte[] outCounter = new byte[]{0x01};
	long value=5000000000L;
	byte[] txOutScriptLength=new byte[]{(byte)0x43};
	byte[] txOutScript=new byte[]{(byte)0x41,(byte)0x04,(byte)0x67,(byte)0x8A,(byte)0xFD,(byte)0xB0,(byte)0xFE,(byte)0x55,(byte)0x48,(byte)0x27,(byte)0x19,(byte)0x67,(byte)0xF1,(byte)0xA6,(byte)0x71,(byte)0x30,(byte)0xB7,(byte)0x10,(byte)0x5C,(byte)0xD6,(byte)0xA8,(byte)0x28,(byte)0xE0,(byte)0x39,(byte)0x09,(byte)0xA6,(byte)0x79,(byte)0x62,(byte)0xE0,(byte)0xEA,(byte)0x1F,(byte)0x61,(byte)0xDE,(byte)0xB6,(byte)0x49,(byte)0xF6,(byte)0xBC,(byte)0x3F,(byte)0x4C,(byte)0xEF,(byte)0x38,(byte)0xC4,(byte)0xF3,(byte)0x55,(byte)0x04,(byte)0xE5,(byte)0x1E,(byte)0xC1,(byte)0x12,(byte)0xDE,(byte)0x5C,(byte)0x38,(byte)0x4D,(byte)0xF7,(byte)0xBA,(byte)0x0B,(byte)0x8D,(byte)0x57,(byte)0x8A,(byte)0x4C,(byte)0x70,(byte)0x2B,(byte)0x6B,(byte)0xF1,(byte)0x1D,(byte)0x5F,(byte)0xAC};
	int lockTime = 0;
	List<TestBitcoinTransactionInput> genesisInput = new ArrayList<TestBitcoinTransactionInput>(1);
	genesisInput.add(new TestBitcoinTransactionInput(previousTransactionHash,previousTxOutIndex,txInScriptLength,txInScript,seqNo));
	List<TestBitcoinTransactionOutput> genesisOutput = new ArrayList<TestBitcoinTransactionOutput>(1);
	genesisOutput.add(new TestBitcoinTransactionOutput(value,txOutScriptLength,txOutScript));
	 TestBitcoinTransaction genesisTransaction = new TestBitcoinTransaction(version,inCounter,genesisInput,outCounter,genesisOutput,lockTime);
	 byte[] expectedHash = BitcoinUtil.reverseByteArray(new byte[]{(byte)0x4A,(byte)0x5E,(byte)0x1E,(byte)0x4B,(byte)0xAA,(byte)0xB8,(byte)0x9F,(byte)0x3A,(byte)0x32,(byte)0x51,(byte)0x8A,(byte)0x88,(byte)0xC3,(byte)0x1B,(byte)0xC8,(byte)0x7F,(byte)0x61,(byte)0x8F,(byte)0x76,(byte)0x67,(byte)0x3E,(byte)0x2C,(byte)0xC7,(byte)0x7A,(byte)0xB2,(byte)0x12,(byte)0x7B,(byte)0x7A,(byte)0xFD,(byte)0xED,(byte)0xA3,(byte)0x3B});
	GenericUDF.DeferredObject[] doa = new GenericUDF.DeferredObject[1];
	doa[0]=new GenericUDF.DeferredJavaObject(genesisTransaction);
	BytesWritable bw = (BytesWritable) bthUDF.evaluate(doa);
	
	assertArrayEquals( expectedHash,bw.copyBytes(),"BitcoinTransaction struct transaction hash from UDF");
  }


@Test
public void BitcoinTransactionHashSegwitUDFInvalidArguments() throws HiveException {
	BitcoinTransactionHashSegwitUDF bthUDF = new BitcoinTransactionHashSegwitUDF();
	UDFArgumentLengthException exNull = assertThrows(UDFArgumentLengthException.class, ()->bthUDF.initialize(null), "Exception is thrown in case of null parameter");
	UDFArgumentLengthException exLen = assertThrows(UDFArgumentLengthException.class, ()->bthUDF.initialize(new ObjectInspector[2]), "Exception is thrown in case of invalid length parameter");
	
	StringObjectInspector[] testStringOI = new StringObjectInspector[1];
	testStringOI[0]=PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	UDFArgumentException wrongType = assertThrows(UDFArgumentException.class, ()->bthUDF.initialize(testStringOI), "Exception is thrown in case of invalid type of parameter");

}

@Test
public void BitcoinTransactionHashSegwitUDFNull() throws HiveException {
	BitcoinTransactionHashSegwitUDF bthUDF = new BitcoinTransactionHashSegwitUDF();
	ObjectInspector[] arguments = new ObjectInspector[1];
	arguments[0] =  ObjectInspectorFactory.getReflectionObjectInspector(BitcoinBlock.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
	bthUDF.initialize(arguments);	
	assertNull(bthUDF.evaluate(null),"Null argument to UDF returns null");
	
	
}


@Test
public void BitcoinTransactionHashSegwitUDFWriteable()  throws HiveException  {
	BitcoinTransactionHashSegwitUDF bthUDF = new BitcoinTransactionHashSegwitUDF();
	ObjectInspector[] arguments = new ObjectInspector[1];
	arguments[0] =  ObjectInspectorFactory.getReflectionObjectInspector(BitcoinTransaction.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
	bthUDF.initialize(arguments);	
// create BitcoinTransaction
	int version=2;
	byte marker=0x00;
	byte flag=0x01;
	byte[] inCounter = new byte[]{0x01};
	byte[] previousTransactionHash = new byte[]{(byte)0x07,(byte)0x21,(byte)0x35,(byte)0x23,(byte)0x6D,(byte)0x2E,(byte)0xBC,(byte)0x78,(byte)0xB6,(byte)0xAC,(byte)0xE1,(byte)0x88,(byte)0x97,(byte)0x03,(byte)0xB1,(byte)0x84,(byte)0x85,(byte)0x52,(byte)0x87,(byte)0x12,(byte)0xBD,(byte)0x70,(byte)0xE0,(byte)0x7F,(byte)0x4A,(byte)0x90,(byte)0x11,(byte)0x40,(byte)0xDE,(byte)0x38,(byte)0xA2,(byte)0xE8};
	long previousTxOutIndex = 1L;

	byte[] txInScriptLength = new byte[]{(byte)0x17};
	
	byte[] txInScript= new byte[]{(byte)0x16,(byte)0x00,(byte)0x14,(byte)0x4D,(byte)0x4D,(byte)0x83,(byte)0xED,(byte)0x5F,(byte)0x10,(byte)0x7B,(byte)0x8D,(byte)0x45,(byte)0x1E,(byte)0x59,(byte)0xA0,(byte)0x43,(byte)0x1A,(byte)0x13,(byte)0x92,(byte)0x79,(byte)0x6B,(byte)0x26,(byte)0x04};
	long seqNo=4294967295L;
	byte[] outCounter = new byte[]{0x02};
	long value_1=1009051983L;
	byte[] txOutScriptLength_1=new byte[]{(byte)0x17};
	byte[] txOutScript_1=new byte[]{(byte)0xA9,(byte)0x14,(byte)0xF0,(byte)0x50,(byte)0xC5,(byte)0x91,(byte)0xEA,(byte)0x98,(byte)0x26,(byte)0x73,(byte)0xCC,(byte)0xED,(byte)0xF5,(byte)0x21,(byte)0x13,(byte)0x65,(byte)0x7B,(byte)0x67,(byte)0x83,(byte)0x03,(byte)0xE6,(byte)0xA1,(byte)0x87};
	long value_2=59801109L;
	byte[] txOutScriptLength_2=new byte[]{(byte)0x19};
	byte[] txOutScript_2=new byte[]{(byte)0x76,(byte)0xA9,(byte)0x14,(byte)0xFB,(byte)0x2E,(byte)0x13,(byte)0x83,(byte)0x5E,(byte)0x39,(byte)0x88,(byte)0xC7,(byte)0x8F,(byte)0x76,(byte)0x0D,(byte)0x4A,(byte)0xC8,(byte)0x1E,(byte)0x04,(byte)0xEA,(byte)0xF1,(byte)0x94,(byte)0xEA,(byte)0x92,(byte)0x88,(byte)0xAC};
	
	// there is only one input so we have only one list of stack items containing 2 items in this case
	byte[] noOfStackItems = new byte[]{0x02};
	byte[] segwitnessLength_1=new byte[]{(byte)0x48};
	byte[] segwitnessScript_1 = new byte[]{(byte)0x30,(byte)0x45,(byte)0x02,(byte)0x21,(byte)0x00,(byte)0xBB,(byte)0x5F,(byte)0x78,(byte)0xE8,(byte)0xA1,(byte)0xBA,(byte)0x5E,(byte)0x14,(byte)0x26,(byte)0x1B,(byte)0x0A,(byte)0xD3,(byte)0x95,(byte)0x56,(byte)0xAF,(byte)0x9B,(byte)0x21,(byte)0xD9,(byte)0x1F,(byte)0x67,(byte)0x5D,(byte)0x38,(byte)0xC8,(byte)0xCD,(byte)0xAD,(byte)0x7E,(byte)0x7F,(byte)0x5D,(byte)0x21,(byte)0x00,(byte)0x4A,(byte)0xBD,(byte)0x02,(byte)0x20,(byte)0x4C,(byte)0x1E,(byte)0xAC,(byte)0xF1,(byte)0xF9,(byte)0xAC,(byte)0x1D,(byte)0xCC,(byte)0x61,(byte)0x63,(byte)0xF2,(byte)0x07,(byte)0xFC,(byte)0xBC,(byte)0x49,(byte)0x8B,(byte)0x32,(byte)0x4C,(byte)0xBE,(byte)0xF5,(byte)0x7F,(byte)0x83,(byte)0x9F,(byte)0xA2,(byte)0xC2,(byte)0x55,(byte)0x57,(byte)0x4B,(byte)0x2F,(byte)0x37,(byte)0x19,(byte)0xBC,(byte)0x01};
	byte[] segwitnessLength_2=new byte[]{(byte)0x21};
	byte[] segwitnessScript_2 = new byte[]{(byte)0x03,(byte)0xC5,(byte)0x3F,(byte)0xEA,(byte)0x9A,(byte)0xE5,(byte)0x61,(byte)0xB6,(byte)0x05,(byte)0x74,(byte)0xB2,(byte)0xD5,(byte)0x10,(byte)0x27,(byte)0x3F,(byte)0x7C,(byte)0x51,(byte)0x60,(byte)0x69,(byte)0x7E,(byte)0xB4,(byte)0x7B,(byte)0x48,(byte)0x8E,(byte)0x95,(byte)0xAD,(byte)0x62,(byte)0x91,(byte)0xBB,(byte)0xCB,(byte)0x5E,(byte)0x43,(byte)0xA2};
	int lockTime = 0;
	List<BitcoinTransactionInput> randomScriptWitnessInput = new ArrayList<BitcoinTransactionInput>(1);
	randomScriptWitnessInput.add(new BitcoinTransactionInput(previousTransactionHash,previousTxOutIndex,txInScriptLength,txInScript,seqNo));
	List<BitcoinTransactionOutput> randomScriptWitnessOutput = new ArrayList<BitcoinTransactionOutput>(2);
	randomScriptWitnessOutput.add(new BitcoinTransactionOutput(value_1,txOutScriptLength_1,txOutScript_1));
	randomScriptWitnessOutput.add(new BitcoinTransactionOutput(value_2,txOutScriptLength_2,txOutScript_2));
	List<BitcoinScriptWitnessItem> randomScriptWitnessSWI = new ArrayList<BitcoinScriptWitnessItem>(1);
	List<BitcoinScriptWitness> randomScriptWitnessSW = new ArrayList<BitcoinScriptWitness>(2);
	randomScriptWitnessSW.add(new BitcoinScriptWitness(segwitnessLength_1,segwitnessScript_1));
	randomScriptWitnessSW.add(new BitcoinScriptWitness(segwitnessLength_2,segwitnessScript_2));
	randomScriptWitnessSWI.add(new BitcoinScriptWitnessItem(noOfStackItems,randomScriptWitnessSW));
	 BitcoinTransaction randomScriptWitnessTransaction = new BitcoinTransaction(marker,flag,version,inCounter,randomScriptWitnessInput,outCounter,randomScriptWitnessOutput,randomScriptWitnessSWI,lockTime);
	 //74700E2CE030013E2E10FCFD06DF99C7826E41C725CA5C467660BFA4874F65BF
	 byte[] expectedHashSegwit = BitcoinUtil.reverseByteArray(new byte[]{(byte)0x74,(byte)0x70,(byte)0x0E,(byte)0x2C,(byte)0xE0,(byte)0x30,(byte)0x01,(byte)0x3E,(byte)0x2E,(byte)0x10,(byte)0xFC,(byte)0xFD,(byte)0x06,(byte)0xDF,(byte)0x99,(byte)0xC7,(byte)0x82,(byte)0x6E,(byte)0x41,(byte)0xC7,(byte)0x25,(byte)0xCA,(byte)0x5C,(byte)0x46,(byte)0x76,(byte)0x60,(byte)0xBF,(byte)0xA4,(byte)0x87,(byte)0x4F,(byte)0x65,(byte)0xBF});


	
	GenericUDF.DeferredObject[] doa = new GenericUDF.DeferredObject[1];
	doa[0]=new GenericUDF.DeferredJavaObject(randomScriptWitnessTransaction);
	BytesWritable bw = (BytesWritable) bthUDF.evaluate(doa);
	
	assertArrayEquals( expectedHashSegwit,bw.copyBytes(),"BitcoinTransaction object random scriptwitness transaction hash segwit from UDF");


}


@Test
public void BitcoinTransactionHashSegwitUDFObjectInspector() throws HiveException {
	BitcoinTransactionHashSegwitUDF bthUDF = new BitcoinTransactionHashSegwitUDF();
	ObjectInspector[] arguments = new ObjectInspector[1];
	arguments[0] =  ObjectInspectorFactory.getReflectionObjectInspector(TestBitcoinTransaction.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
	bthUDF.initialize(arguments);	
	// create Segwit transaction
	int version=2;
	byte marker=0x00;
	byte flag=0x01;
	byte[] inCounter = new byte[]{0x01};
	byte[] previousTransactionHash = new byte[]{(byte)0x07,(byte)0x21,(byte)0x35,(byte)0x23,(byte)0x6D,(byte)0x2E,(byte)0xBC,(byte)0x78,(byte)0xB6,(byte)0xAC,(byte)0xE1,(byte)0x88,(byte)0x97,(byte)0x03,(byte)0xB1,(byte)0x84,(byte)0x85,(byte)0x52,(byte)0x87,(byte)0x12,(byte)0xBD,(byte)0x70,(byte)0xE0,(byte)0x7F,(byte)0x4A,(byte)0x90,(byte)0x11,(byte)0x40,(byte)0xDE,(byte)0x38,(byte)0xA2,(byte)0xE8};
	long previousTxOutIndex = 1L;

	byte[] txInScriptLength = new byte[]{(byte)0x17};
	
	byte[] txInScript= new byte[]{(byte)0x16,(byte)0x00,(byte)0x14,(byte)0x4D,(byte)0x4D,(byte)0x83,(byte)0xED,(byte)0x5F,(byte)0x10,(byte)0x7B,(byte)0x8D,(byte)0x45,(byte)0x1E,(byte)0x59,(byte)0xA0,(byte)0x43,(byte)0x1A,(byte)0x13,(byte)0x92,(byte)0x79,(byte)0x6B,(byte)0x26,(byte)0x04};
	long seqNo=4294967295L;
	byte[] outCounter = new byte[]{0x02};
	long value_1=1009051983L;
	byte[] txOutScriptLength_1=new byte[]{(byte)0x17};
	byte[] txOutScript_1=new byte[]{(byte)0xA9,(byte)0x14,(byte)0xF0,(byte)0x50,(byte)0xC5,(byte)0x91,(byte)0xEA,(byte)0x98,(byte)0x26,(byte)0x73,(byte)0xCC,(byte)0xED,(byte)0xF5,(byte)0x21,(byte)0x13,(byte)0x65,(byte)0x7B,(byte)0x67,(byte)0x83,(byte)0x03,(byte)0xE6,(byte)0xA1,(byte)0x87};
	long value_2=59801109L;
	byte[] txOutScriptLength_2=new byte[]{(byte)0x19};
	byte[] txOutScript_2=new byte[]{(byte)0x76,(byte)0xA9,(byte)0x14,(byte)0xFB,(byte)0x2E,(byte)0x13,(byte)0x83,(byte)0x5E,(byte)0x39,(byte)0x88,(byte)0xC7,(byte)0x8F,(byte)0x76,(byte)0x0D,(byte)0x4A,(byte)0xC8,(byte)0x1E,(byte)0x04,(byte)0xEA,(byte)0xF1,(byte)0x94,(byte)0xEA,(byte)0x92,(byte)0x88,(byte)0xAC};
	
	// there is only one input so we have only one list of stack items containing 2 items in this case
	byte[] noOfStackItems = new byte[]{0x02};
	byte[] segwitnessLength_1=new byte[]{(byte)0x48};
	byte[] segwitnessScript_1 = new byte[]{(byte)0x30,(byte)0x45,(byte)0x02,(byte)0x21,(byte)0x00,(byte)0xBB,(byte)0x5F,(byte)0x78,(byte)0xE8,(byte)0xA1,(byte)0xBA,(byte)0x5E,(byte)0x14,(byte)0x26,(byte)0x1B,(byte)0x0A,(byte)0xD3,(byte)0x95,(byte)0x56,(byte)0xAF,(byte)0x9B,(byte)0x21,(byte)0xD9,(byte)0x1F,(byte)0x67,(byte)0x5D,(byte)0x38,(byte)0xC8,(byte)0xCD,(byte)0xAD,(byte)0x7E,(byte)0x7F,(byte)0x5D,(byte)0x21,(byte)0x00,(byte)0x4A,(byte)0xBD,(byte)0x02,(byte)0x20,(byte)0x4C,(byte)0x1E,(byte)0xAC,(byte)0xF1,(byte)0xF9,(byte)0xAC,(byte)0x1D,(byte)0xCC,(byte)0x61,(byte)0x63,(byte)0xF2,(byte)0x07,(byte)0xFC,(byte)0xBC,(byte)0x49,(byte)0x8B,(byte)0x32,(byte)0x4C,(byte)0xBE,(byte)0xF5,(byte)0x7F,(byte)0x83,(byte)0x9F,(byte)0xA2,(byte)0xC2,(byte)0x55,(byte)0x57,(byte)0x4B,(byte)0x2F,(byte)0x37,(byte)0x19,(byte)0xBC,(byte)0x01};
	byte[] segwitnessLength_2=new byte[]{(byte)0x21};
	byte[] segwitnessScript_2 = new byte[]{(byte)0x03,(byte)0xC5,(byte)0x3F,(byte)0xEA,(byte)0x9A,(byte)0xE5,(byte)0x61,(byte)0xB6,(byte)0x05,(byte)0x74,(byte)0xB2,(byte)0xD5,(byte)0x10,(byte)0x27,(byte)0x3F,(byte)0x7C,(byte)0x51,(byte)0x60,(byte)0x69,(byte)0x7E,(byte)0xB4,(byte)0x7B,(byte)0x48,(byte)0x8E,(byte)0x95,(byte)0xAD,(byte)0x62,(byte)0x91,(byte)0xBB,(byte)0xCB,(byte)0x5E,(byte)0x43,(byte)0xA2};
	int lockTime = 0;
	List<TestBitcoinTransactionInput> randomScriptWitnessInput = new ArrayList<TestBitcoinTransactionInput>(1);
	randomScriptWitnessInput.add(new TestBitcoinTransactionInput(previousTransactionHash,previousTxOutIndex,txInScriptLength,txInScript,seqNo));
	List<TestBitcoinTransactionOutput> randomScriptWitnessOutput = new ArrayList<TestBitcoinTransactionOutput>(2);
	randomScriptWitnessOutput.add(new TestBitcoinTransactionOutput(value_1,txOutScriptLength_1,txOutScript_1));
	randomScriptWitnessOutput.add(new TestBitcoinTransactionOutput(value_2,txOutScriptLength_2,txOutScript_2));
	List<TestBitcoinScriptWitnessItem> randomScriptWitnessSWI = new ArrayList<TestBitcoinScriptWitnessItem>(1);
	List<TestBitcoinScriptWitness> randomScriptWitnessSW = new ArrayList<TestBitcoinScriptWitness>(2);
	randomScriptWitnessSW.add(new TestBitcoinScriptWitness(segwitnessLength_1,segwitnessScript_1));
	randomScriptWitnessSW.add(new TestBitcoinScriptWitness(segwitnessLength_2,segwitnessScript_2));
	randomScriptWitnessSWI.add(new TestBitcoinScriptWitnessItem(noOfStackItems,randomScriptWitnessSW));
	 TestBitcoinTransaction randomScriptWitnessTransaction = new TestBitcoinTransaction(marker,flag,version,inCounter,randomScriptWitnessInput,outCounter,randomScriptWitnessOutput,randomScriptWitnessSWI,lockTime);
	 
	 //74700E2CE030013E2E10FCFD06DF99C7826E41C725CA5C467660BFA4874F65BF
	 byte[] expectedHashSegwit = BitcoinUtil.reverseByteArray(new byte[]{(byte)0x74,(byte)0x70,(byte)0x0E,(byte)0x2C,(byte)0xE0,(byte)0x30,(byte)0x01,(byte)0x3E,(byte)0x2E,(byte)0x10,(byte)0xFC,(byte)0xFD,(byte)0x06,(byte)0xDF,(byte)0x99,(byte)0xC7,(byte)0x82,(byte)0x6E,(byte)0x41,(byte)0xC7,(byte)0x25,(byte)0xCA,(byte)0x5C,(byte)0x46,(byte)0x76,(byte)0x60,(byte)0xBF,(byte)0xA4,(byte)0x87,(byte)0x4F,(byte)0x65,(byte)0xBF});
	 GenericUDF.DeferredObject[] doa = new GenericUDF.DeferredObject[1];
		doa[0]=new GenericUDF.DeferredJavaObject(randomScriptWitnessTransaction);
	 BytesWritable bw = (BytesWritable) bthUDF.evaluate(doa);
		
		assertArrayEquals( expectedHashSegwit,bw.copyBytes(),"BitcoinTransaction struct transaction hash segwit from UDF");
}
} 
