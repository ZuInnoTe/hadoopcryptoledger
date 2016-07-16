package org.zuinnote.hadoop.bitcoin.hive.udf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable; 
import org.apache.hadoop.io.Text; 

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import  org.apache.hadoop.hive.ql.udf.generic.*;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;

import org.zuinnote.hadoop.bitcoin.hive.udf.*;
import org.zuinnote.hadoop.bitcoin.format.*;

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
	assertEquals("TxOutScript from Genesis should be payment to a pubkey address", comparatorText,result);
  }


@Test
  public void BitcoinScriptPaymentPatternAnalyzerUDFNull() {
	BitcoinScriptPaymentPatternAnalyzerUDF bsppaUDF = new BitcoinScriptPaymentPatternAnalyzerUDF();
	assertNull("Null argument to UDF returns null", bsppaUDF.evaluate(null));
  }

 
@Test
  public void BitcoinTransactionHashUDFNull() throws HiveException {
	BitcoinTransactionHashUDF bthUDF = new BitcoinTransactionHashUDF();
	ObjectInspector[] arguments = new ObjectInspector[1];
	arguments[0] =  ObjectInspectorFactory.getReflectionObjectInspector(BitcoinBlock.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
	bthUDF.initialize(arguments);	
	assertNull("Null argument to UDF returns null",bthUDF.evaluate(null));
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
	 byte[] expectedHash = new byte[]{(byte)0x4A,(byte)0x5E,(byte)0x1E,(byte)0x4B,(byte)0xAA,(byte)0xB8,(byte)0x9F,(byte)0x3A,(byte)0x32,(byte)0x51,(byte)0x8A,(byte)0x88,(byte)0xC3,(byte)0x1B,(byte)0xC8,(byte)0x7F,(byte)0x61,(byte)0x8F,(byte)0x76,(byte)0x67,(byte)0x3E,(byte)0x2C,(byte)0xC7,(byte)0x7A,(byte)0xB2,(byte)0x12,(byte)0x7B,(byte)0x7A,(byte)0xFD,(byte)0xED,(byte)0xA3,(byte)0x3B};
	GenericUDF.DeferredObject[] doa = new GenericUDF.DeferredObject[1];
	doa[0]=new GenericUDF.DeferredJavaObject(genesisTransaction);
	BytesWritable bw = (BytesWritable) bthUDF.evaluate(doa);
	
	assertArrayEquals("BitcoinTransaction object genesis transaction hash from UDF", expectedHash,bw.copyBytes());
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
	 byte[] expectedHash = new byte[]{(byte)0x4A,(byte)0x5E,(byte)0x1E,(byte)0x4B,(byte)0xAA,(byte)0xB8,(byte)0x9F,(byte)0x3A,(byte)0x32,(byte)0x51,(byte)0x8A,(byte)0x88,(byte)0xC3,(byte)0x1B,(byte)0xC8,(byte)0x7F,(byte)0x61,(byte)0x8F,(byte)0x76,(byte)0x67,(byte)0x3E,(byte)0x2C,(byte)0xC7,(byte)0x7A,(byte)0xB2,(byte)0x12,(byte)0x7B,(byte)0x7A,(byte)0xFD,(byte)0xED,(byte)0xA3,(byte)0x3B};
	GenericUDF.DeferredObject[] doa = new GenericUDF.DeferredObject[1];
	doa[0]=new GenericUDF.DeferredJavaObject(genesisTransaction);
	BytesWritable bw = (BytesWritable) bthUDF.evaluate(doa);
	
	assertArrayEquals("BitcoinTransaction struct transaction hash from UDF", expectedHash,bw.copyBytes());
  }

} 
