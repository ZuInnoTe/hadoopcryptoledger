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
package org.zuinnote.hadoop.ethereum.hive.udf;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.jupiter.api.Test;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import org.zuinnote.hadoop.ethereum.format.common.EthereumBlock;
import org.zuinnote.hadoop.ethereum.format.common.EthereumBlockReader;
import org.zuinnote.hadoop.ethereum.format.common.EthereumTransaction;
import org.zuinnote.hadoop.ethereum.format.exception.EthereumBlockReadException;

/**
 *
 *
 */
public class EthereumUDFTest {
	static final int DEFAULT_BUFFERSIZE=64*1024;
	static final int DEFAULT_MAXSIZE_ETHEREUMBLOCK=1 * 1024 * 1024;
	
	  
	  @Test
	  public void checkTestDataBlock1346406Available() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="eth1346406.bin";
		String fileNameGenesis=classLoader.getResource(fileName).getFile();	
		assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameGenesis);
		File file = new File(fileNameGenesis);
		assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
		assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
	  }
	  
	  @Test
	  public void EthereumGetChainIdUDFNull() throws HiveException {
		EthereumGetChainIdUDF egcidUDF = new EthereumGetChainIdUDF();
		ObjectInspector[] arguments = new ObjectInspector[1];
		arguments[0] =  ObjectInspectorFactory.getReflectionObjectInspector(EthereumTransaction.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		egcidUDF.initialize(arguments);	
		assertNull(egcidUDF.evaluate(null),"Null argument to UDF returns null");
	  }
	  
	  @Test
	  public void EthereumGetSendAddressUDFNull() throws HiveException {
		  EthereumGetSendAddressUDF egsaUDF = new EthereumGetSendAddressUDF();
		ObjectInspector[] arguments = new ObjectInspector[1];
		arguments[0] =  ObjectInspectorFactory.getReflectionObjectInspector(EthereumTransaction.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		egsaUDF.initialize(arguments);	
		assertNull(egsaUDF.evaluate(null),"Null argument to UDF returns null");
	  }
	  
	  @Test
	  public void EthereumGetTransactionHashUDFNull() throws HiveException {
		  EthereumGetTransactionHashUDF egthUDF = new EthereumGetTransactionHashUDF();
		ObjectInspector[] arguments = new ObjectInspector[1];
		arguments[0] =  ObjectInspectorFactory.getReflectionObjectInspector(EthereumTransaction.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		egthUDF.initialize(arguments);	
		assertNull(egthUDF.evaluate(null),"Null argument to UDF returns null");
	  }
	  
	  @Test
	  public void EthereumGetChainIdUDFWritable() throws HiveException, IOException, EthereumBlockReadException {
		  // initialize object inspector
			EthereumGetChainIdUDF egcidUDF = new EthereumGetChainIdUDF();
			ObjectInspector[] arguments = new ObjectInspector[1];
			arguments[0] =  ObjectInspectorFactory.getReflectionObjectInspector(EthereumTransaction.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			egcidUDF.initialize(arguments);	
			// load test data
		  ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth1346406.bin";
			String fileNameBlock=classLoader.getResource(fileName).getFile();	
			File file = new File(fileNameBlock);
			boolean direct=false;
			FileInputStream fin = new FileInputStream(file);
			EthereumBlockReader ebr = null;
			try {
				ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
				EthereumBlock eblock = ebr.readBlock();
				List<EthereumTransaction> eTrans = eblock.getEthereumTransactions();
				// validate UDFs
				EthereumTransaction trans0 = eTrans.get(0);

				assertNull(egcidUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans0)}),"Block 1346406 Transaction 1 is Ethereum MainNet");
				EthereumTransaction trans1 = eTrans.get(1);
				assertNull(egcidUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans1)}),"Block 1346406 Transaction 2 is Ethereum MainNet");
				EthereumTransaction trans2 = eTrans.get(2);
				assertNull(egcidUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans2)}),"Block 1346406 Transaction 3 is Ethereum MainNet");
				EthereumTransaction trans3 = eTrans.get(3);
				assertNull(egcidUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans3)}),"Block 1346406 Transaction 4 is Ethereum MainNet");
				EthereumTransaction trans4 = eTrans.get(4);
				assertNull(egcidUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans4)}),"Block 1346406 Transaction 5 is Ethereum MainNet");
				EthereumTransaction trans5 = eTrans.get(5);
				assertNull(egcidUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans5)}),"Block 1346406 Transaction 6 is Ethereum MainNet");
			}
			 finally {
				if (ebr!=null) {
					ebr.close();
				}
			}

	  }
	  
	  @Test
	  public void EthereumGetChainIdUDFObejctInspector() throws HiveException, IOException, EthereumBlockReadException {
		  // initialize object inspector
			EthereumGetChainIdUDF egcidUDF = new EthereumGetChainIdUDF();
			ObjectInspector[] arguments = new ObjectInspector[1];
			arguments[0] =  ObjectInspectorFactory.getReflectionObjectInspector(TestEthereumTransaction.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			egcidUDF.initialize(arguments);	
			// load test data
		  ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth1346406.bin";
			String fileNameBlock=classLoader.getResource(fileName).getFile();	
			File file = new File(fileNameBlock);
			boolean direct=false;
			FileInputStream fin = new FileInputStream(file);
			EthereumBlockReader ebr = null;
			try {
				ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
				EthereumBlock eblock = ebr.readBlock();
				List<EthereumTransaction> eTrans = eblock.getEthereumTransactions();
				// validate UDFs
				EthereumTransaction transOrig0 = eTrans.get(0);
				TestEthereumTransaction trans0 = new TestEthereumTransaction();
				trans0.set(transOrig0);
				assertNull(egcidUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans0)}),"Block 1346406 Transaction 1 is Ethereum MainNet");
				EthereumTransaction transOrig1 = eTrans.get(1);
				TestEthereumTransaction trans1 = new TestEthereumTransaction();
				trans1.set(transOrig1);
				assertNull(egcidUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans1)}),"Block 1346406 Transaction 2 is Ethereum MainNet");
				EthereumTransaction transOrig2 = eTrans.get(2);
				TestEthereumTransaction trans2 = new TestEthereumTransaction();
				trans2.set(transOrig2);
				assertNull(egcidUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans2)}),"Block 1346406 Transaction 3 is Ethereum MainNet");
				EthereumTransaction transOrig3 = eTrans.get(3);
				TestEthereumTransaction trans3 = new TestEthereumTransaction();
				trans3.set(transOrig3);
				assertNull(egcidUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans3)}),"Block 1346406 Transaction 4 is Ethereum MainNet");
				EthereumTransaction transOrig4 = eTrans.get(4);
				TestEthereumTransaction trans4 = new TestEthereumTransaction();
				trans4.set(transOrig4);
				assertNull(egcidUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans4)}),"Block 1346406 Transaction 5 is Ethereum MainNet");
				EthereumTransaction transOrig5 = eTrans.get(5);
				TestEthereumTransaction trans5 = new TestEthereumTransaction();
				trans5.set(transOrig5);
				assertNull(egcidUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans5)}),"Block 1346406 Transaction 6 is Ethereum MainNet");
			}
			 finally {
				if (ebr!=null) {
					ebr.close();
				}
			}

	  }
	  
	  @Test
	  public void EthereumGetSendAddressUDFWritable() throws HiveException, IOException, EthereumBlockReadException {
		  // initialize object inspector
		  EthereumGetSendAddressUDF egsaUDF = new EthereumGetSendAddressUDF();
			ObjectInspector[] arguments = new ObjectInspector[1];
			arguments[0] =  ObjectInspectorFactory.getReflectionObjectInspector(EthereumTransaction.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			egsaUDF.initialize(arguments);	
			// load test data
		  ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth1346406.bin";
			String fileNameBlock=classLoader.getResource(fileName).getFile();	
			File file = new File(fileNameBlock);
			boolean direct=false;
			FileInputStream fin = new FileInputStream(file);
			EthereumBlockReader ebr = null;
			try {
				ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
				EthereumBlock eblock = ebr.readBlock();
				List<EthereumTransaction> eTrans = eblock.getEthereumTransactions();
				// validate UDFs
				
				 EthereumTransaction trans0 = eTrans.get(0);
			      byte[] expectedSentAddress = new byte[] {(byte)0x39,(byte)0x42,(byte)0x4b,(byte)0xd2,(byte)0x8a,(byte)0x22,(byte)0x23,(byte)0xda,(byte)0x3e,(byte)0x14,(byte)0xbf,(byte)0x79,(byte)0x3c,(byte)0xf7,(byte)0xf8,(byte)0x20,(byte)0x8e,(byte)0xe9,(byte)0x98,(byte)0x0a};
			      assertArrayEquals(expectedSentAddress,((BytesWritable)egsaUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans0)})).copyBytes(),"Block 1346406 Transaction 1 send address is correctly calculated");
			      EthereumTransaction trans1 = eTrans.get(1);
			      expectedSentAddress = new byte[] {(byte)0x4b,(byte)0xb9,(byte)0x60,(byte)0x91,(byte)0xee,(byte)0x9d,(byte)0x80,(byte)0x2e,(byte)0xd0,(byte)0x39,(byte)0xc4,(byte)0xd1,(byte)0xa5,(byte)0xf6,(byte)0x21,(byte)0x6f,(byte)0x90,(byte)0xf8,(byte)0x1b,(byte)0x01};
			      assertArrayEquals(expectedSentAddress,((BytesWritable)egsaUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans1)})).copyBytes(),"Block 1346406 Transaction 2 send address is correctly calculated");
			      EthereumTransaction trans2 = eTrans.get(2);
			      expectedSentAddress = new byte[] {(byte)0x63,(byte)0xa9,(byte)0x97,(byte)0x5b,(byte)0xa3,(byte)0x1b,(byte)0x0b,(byte)0x96,(byte)0x26,(byte)0xb3,(byte)0x43,(byte)0x00,(byte)0xf7,(byte)0xf6,(byte)0x27,(byte)0x14,(byte)0x7d,(byte)0xf1,(byte)0xf5,(byte)0x26};
			      assertArrayEquals(expectedSentAddress,((BytesWritable)egsaUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans2)})).copyBytes(),"Block 1346406 Transaction 3 send address is correctly calculated");
			      EthereumTransaction trans3 = eTrans.get(3);
			      expectedSentAddress = new byte[] {(byte)0x63,(byte)0xa9,(byte)0x97,(byte)0x5b,(byte)0xa3,(byte)0x1b,(byte)0x0b,(byte)0x96,(byte)0x26,(byte)0xb3,(byte)0x43,(byte)0x00,(byte)0xf7,(byte)0xf6,(byte)0x27,(byte)0x14,(byte)0x7d,(byte)0xf1,(byte)0xf5,(byte)0x26};
			     assertArrayEquals(expectedSentAddress,((BytesWritable)egsaUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans3)})).copyBytes(),"Block 1346406 Transaction 4 send address is correctly calculated");
			      EthereumTransaction trans4 = eTrans.get(4);
			      expectedSentAddress = new byte[] {(byte)0x63,(byte)0xa9,(byte)0x97,(byte)0x5b,(byte)0xa3,(byte)0x1b,(byte)0x0b,(byte)0x96,(byte)0x26,(byte)0xb3,(byte)0x43,(byte)0x00,(byte)0xf7,(byte)0xf6,(byte)0x27,(byte)0x14,(byte)0x7d,(byte)0xf1,(byte)0xf5,(byte)0x26};
			      assertArrayEquals(expectedSentAddress,((BytesWritable)egsaUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans4)})).copyBytes(),"Block 1346406 Transaction 5 send address is correctly calculated");
			      EthereumTransaction trans5 = eTrans.get(5);
			      expectedSentAddress = new byte[] {(byte)0x63,(byte)0xa9,(byte)0x97,(byte)0x5b,(byte)0xa3,(byte)0x1b,(byte)0x0b,(byte)0x96,(byte)0x26,(byte)0xb3,(byte)0x43,(byte)0x00,(byte)0xf7,(byte)0xf6,(byte)0x27,(byte)0x14,(byte)0x7d,(byte)0xf1,(byte)0xf5,(byte)0x26};
			      assertArrayEquals(expectedSentAddress,((BytesWritable)egsaUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans5)})).copyBytes(),"Block 1346406 Transaction 6 send address is correctly calculated");
			      
		}
			 finally {
				if (ebr!=null) {
					ebr.close();
				}
			}

	  }
	  
	  @Test
	  public void EthereumGetSendAddressUDFObjectInspector() throws HiveException, IOException, EthereumBlockReadException {
		  // initialize object inspector
		  EthereumGetSendAddressUDF egsaUDF = new EthereumGetSendAddressUDF();
			ObjectInspector[] arguments = new ObjectInspector[1];
			arguments[0] =  ObjectInspectorFactory.getReflectionObjectInspector(TestEthereumTransaction.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			egsaUDF.initialize(arguments);	
			// load test data
		  ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth1346406.bin";
			String fileNameBlock=classLoader.getResource(fileName).getFile();	
			File file = new File(fileNameBlock);
			boolean direct=false;
			FileInputStream fin = new FileInputStream(file);
			EthereumBlockReader ebr = null;
			try {
				ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
				EthereumBlock eblock = ebr.readBlock();
				List<EthereumTransaction> eTrans = eblock.getEthereumTransactions();
				// validate UDFs
				EthereumTransaction transOrig0 = eTrans.get(0);
				TestEthereumTransaction trans0 = new TestEthereumTransaction();
				trans0.set(transOrig0);
			      byte[] expectedSentAddress = new byte[] {(byte)0x39,(byte)0x42,(byte)0x4b,(byte)0xd2,(byte)0x8a,(byte)0x22,(byte)0x23,(byte)0xda,(byte)0x3e,(byte)0x14,(byte)0xbf,(byte)0x79,(byte)0x3c,(byte)0xf7,(byte)0xf8,(byte)0x20,(byte)0x8e,(byte)0xe9,(byte)0x98,(byte)0x0a};
			      assertArrayEquals(expectedSentAddress,((BytesWritable)egsaUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans0)})).copyBytes(),"Block 1346406 Transaction 1 send address is correctly calculated");
					EthereumTransaction transOrig1 = eTrans.get(1);
					TestEthereumTransaction trans1 = new TestEthereumTransaction();
					trans1.set(transOrig1);
			      expectedSentAddress = new byte[] {(byte)0x4b,(byte)0xb9,(byte)0x60,(byte)0x91,(byte)0xee,(byte)0x9d,(byte)0x80,(byte)0x2e,(byte)0xd0,(byte)0x39,(byte)0xc4,(byte)0xd1,(byte)0xa5,(byte)0xf6,(byte)0x21,(byte)0x6f,(byte)0x90,(byte)0xf8,(byte)0x1b,(byte)0x01};
			      assertArrayEquals(expectedSentAddress,((BytesWritable)egsaUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans1)})).copyBytes(),"Block 1346406 Transaction 2 send address is correctly calculated");
					EthereumTransaction transOrig2 = eTrans.get(2);
					TestEthereumTransaction trans2 = new TestEthereumTransaction();
					trans2.set(transOrig2);
			      expectedSentAddress = new byte[] {(byte)0x63,(byte)0xa9,(byte)0x97,(byte)0x5b,(byte)0xa3,(byte)0x1b,(byte)0x0b,(byte)0x96,(byte)0x26,(byte)0xb3,(byte)0x43,(byte)0x00,(byte)0xf7,(byte)0xf6,(byte)0x27,(byte)0x14,(byte)0x7d,(byte)0xf1,(byte)0xf5,(byte)0x26};
			      assertArrayEquals(expectedSentAddress,((BytesWritable)egsaUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans2)})).copyBytes(),"Block 1346406 Transaction 3 send address is correctly calculated");
					EthereumTransaction transOrig3 = eTrans.get(3);
					TestEthereumTransaction trans3 = new TestEthereumTransaction();
					trans3.set(transOrig3);
			      expectedSentAddress = new byte[] {(byte)0x63,(byte)0xa9,(byte)0x97,(byte)0x5b,(byte)0xa3,(byte)0x1b,(byte)0x0b,(byte)0x96,(byte)0x26,(byte)0xb3,(byte)0x43,(byte)0x00,(byte)0xf7,(byte)0xf6,(byte)0x27,(byte)0x14,(byte)0x7d,(byte)0xf1,(byte)0xf5,(byte)0x26};
			     assertArrayEquals(expectedSentAddress,((BytesWritable)egsaUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans3)})).copyBytes(),"Block 1346406 Transaction 4 send address is correctly calculated");
					EthereumTransaction transOrig4 = eTrans.get(4);
					TestEthereumTransaction trans4 = new TestEthereumTransaction();
					trans4.set(transOrig4);
			      expectedSentAddress = new byte[] {(byte)0x63,(byte)0xa9,(byte)0x97,(byte)0x5b,(byte)0xa3,(byte)0x1b,(byte)0x0b,(byte)0x96,(byte)0x26,(byte)0xb3,(byte)0x43,(byte)0x00,(byte)0xf7,(byte)0xf6,(byte)0x27,(byte)0x14,(byte)0x7d,(byte)0xf1,(byte)0xf5,(byte)0x26};
			      assertArrayEquals(expectedSentAddress,((BytesWritable)egsaUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans4)})).copyBytes(),"Block 1346406 Transaction 5 send address is correctly calculated");
					EthereumTransaction transOrig5 = eTrans.get(5);
					TestEthereumTransaction trans5 = new TestEthereumTransaction();
					trans5.set(transOrig5);
			      expectedSentAddress = new byte[] {(byte)0x63,(byte)0xa9,(byte)0x97,(byte)0x5b,(byte)0xa3,(byte)0x1b,(byte)0x0b,(byte)0x96,(byte)0x26,(byte)0xb3,(byte)0x43,(byte)0x00,(byte)0xf7,(byte)0xf6,(byte)0x27,(byte)0x14,(byte)0x7d,(byte)0xf1,(byte)0xf5,(byte)0x26};
			      assertArrayEquals(expectedSentAddress,((BytesWritable)egsaUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans5)})).copyBytes(),"Block 1346406 Transaction 6 send address is correctly calculated");
			      
		}
			 finally {
				if (ebr!=null) {
					ebr.close();
				}
			}

	  }
	  
	  @Test
	  public void EthereumGetTransactionHashUDFWritable() throws HiveException, IOException, EthereumBlockReadException {
		  // initialize object inspector
		  EthereumGetTransactionHashUDF egthUDF = new EthereumGetTransactionHashUDF();
			ObjectInspector[] arguments = new ObjectInspector[1];
			arguments[0] =  ObjectInspectorFactory.getReflectionObjectInspector(EthereumTransaction.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			egthUDF.initialize(arguments);	
			// load test data
		  ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth1346406.bin";
			String fileNameBlock=classLoader.getResource(fileName).getFile();	
			File file = new File(fileNameBlock);
			boolean direct=false;
			FileInputStream fin = new FileInputStream(file);
			EthereumBlockReader ebr = null;
			try {
				ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
				EthereumBlock eblock = ebr.readBlock();
				List<EthereumTransaction> eTrans = eblock.getEthereumTransactions();
				// validate UDFs
				EthereumTransaction trans0 = eTrans.get(0);
				byte[] expectedHash = new byte[] {(byte)0xe2,(byte)0x7e,(byte)0x92,(byte)0x88,(byte)0xe2,(byte)0x9c,(byte)0xc8,(byte)0xeb,(byte)0x78,(byte)0xf9,(byte)0xf7,(byte)0x68,(byte)0xd8,(byte)0x9b,(byte)0xf1,(byte)0xcd,(byte)0x4b,(byte)0x68,(byte)0xb7,(byte)0x15,(byte)0xa3,(byte)0x8b,(byte)0x95,(byte)0xd4,(byte)0x6d,(byte)0x77,(byte)0x86,(byte)0x18,(byte)0xcb,(byte)0x10,(byte)0x4d,(byte)0x58};
				assertArrayEquals(expectedHash,((BytesWritable)egthUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans0)})).copyBytes(),"Block 1346406 Transaction 1 hash is correctly calculated");
				EthereumTransaction trans1 = eTrans.get(1);
				expectedHash = new byte[] {(byte)0x7a,(byte)0x23,(byte)0x2a,(byte)0xa2,(byte)0xae,(byte)0x6a,(byte)0x5e,(byte)0x1f,(byte)0x32,(byte)0xca,(byte)0x3a,(byte)0xc9,(byte)0x3f,(byte)0x4f,(byte)0xdb,(byte)0x77,(byte)0x98,(byte)0x3e,(byte)0x93,(byte)0x2b,(byte)0x38,(byte)0x09,(byte)0x93,(byte)0x56,(byte)0x44,(byte)0x42,(byte)0x08,(byte)0xc6,(byte)0x9d,(byte)0x40,(byte)0x86,(byte)0x81};
				assertArrayEquals(expectedHash,((BytesWritable)egthUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans1)})).copyBytes(),"Block 1346406 Transaction 2 hash is correctly calculated");
				EthereumTransaction trans2 = eTrans.get(2);
				expectedHash = new byte[] {(byte)0x14,(byte)0x33,(byte)0xe3,(byte)0xcb,(byte)0x66,(byte)0x2f,(byte)0x66,(byte)0x8d,(byte)0x87,(byte)0xb8,(byte)0x35,(byte)0x55,(byte)0x34,(byte)0x5a,(byte)0x20,(byte)0xcc,(byte)0xf8,(byte)0x70,(byte)0x6f,(byte)0x25,(byte)0x21,(byte)0x49,(byte)0x18,(byte)0xe2,(byte)0xf8,(byte)0x1f,(byte)0xe3,(byte)0xd2,(byte)0x1c,(byte)0x9d,(byte)0x5b,(byte)0x23};
				assertArrayEquals(expectedHash,((BytesWritable)egthUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans2)})).copyBytes(),"Block 1346406 Transaction 3 hash is correctly calculated");
				EthereumTransaction trans3 = eTrans.get(3);
				expectedHash = new byte[] {(byte)0x39,(byte)0x22,(byte)0xf7,(byte)0xf6,(byte)0x0a,(byte)0x33,(byte)0xa1,(byte)0x2d,(byte)0x13,(byte)0x9d,(byte)0x67,(byte)0xfa,(byte)0x53,(byte)0x30,(byte)0xdb,(byte)0xfd,(byte)0xba,(byte)0x42,(byte)0xa4,(byte)0xb7,(byte)0x67,(byte)0x29,(byte)0x6e,(byte)0xff,(byte)0x64,(byte)0x15,(byte)0xee,(byte)0xa3,(byte)0x2d,(byte)0x8a,(byte)0x7b,(byte)0x2b};
				assertArrayEquals(expectedHash,((BytesWritable)egthUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans3)})).copyBytes(),"Block 1346406 Transaction 4 hash is correctly calculated");
				EthereumTransaction trans4 = eTrans.get(4);
				expectedHash = new byte[] {(byte)0xbb,(byte)0x7c,(byte)0xaa,(byte)0x23,(byte)0x38,(byte)0x5a,(byte)0x0f,(byte)0x73,(byte)0x75,(byte)0x3f,(byte)0x9e,(byte)0x28,(byte)0xd8,(byte)0xf0,(byte)0x60,(byte)0x2f,(byte)0xe2,(byte)0xe7,(byte)0x2d,(byte)0x87,(byte)0xe1,(byte)0xe0,(byte)0x95,(byte)0x52,(byte)0x75,(byte)0x28,(byte)0xd1,(byte)0x44,(byte)0x88,(byte)0x5d,(byte)0x6b,(byte)0x51};
				assertArrayEquals(expectedHash,((BytesWritable)egthUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans4)})).copyBytes(),"Block 1346406 Transaction 5 hash is correctly calculated");
				EthereumTransaction trans5 = eTrans.get(5);
				expectedHash = new byte[] {(byte)0xbc,(byte)0xde,(byte)0x6f,(byte)0x49,(byte)0x84,(byte)0x2c,(byte)0x6d,(byte)0x73,(byte)0x8d,(byte)0x64,(byte)0x32,(byte)0x8f,(byte)0x78,(byte)0x09,(byte)0xb1,(byte)0xd4,(byte)0x9b,(byte)0xf0,(byte)0xff,(byte)0x3f,(byte)0xfa,(byte)0x46,(byte)0x0f,(byte)0xdd,(byte)0xd2,(byte)0x7f,(byte)0xd4,(byte)0x2b,(byte)0x7a,(byte)0x01,(byte)0xfc,(byte)0x9a};
				assertArrayEquals(expectedHash,((BytesWritable)egthUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans5)})).copyBytes(),"Block 1346406 Transaction 6 hash is correctly calculated");
				    
		}
			 finally {
				if (ebr!=null) {
					ebr.close();
				}
			}

	  }
	  
	  @Test
	  public void EthereumGetTransactionHashUDFObjectInspector() throws HiveException, IOException, EthereumBlockReadException {
		  // initialize object inspector
		  EthereumGetTransactionHashUDF egthUDF = new EthereumGetTransactionHashUDF();
			ObjectInspector[] arguments = new ObjectInspector[1];
			arguments[0] =  ObjectInspectorFactory.getReflectionObjectInspector(TestEthereumTransaction.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			egthUDF.initialize(arguments);	
			// load test data
		  ClassLoader classLoader = getClass().getClassLoader();
			String fileName="eth1346406.bin";
			String fileNameBlock=classLoader.getResource(fileName).getFile();	
			File file = new File(fileNameBlock);
			boolean direct=false;
			FileInputStream fin = new FileInputStream(file);
			EthereumBlockReader ebr = null;
			try {
				ebr = new EthereumBlockReader(fin,this.DEFAULT_MAXSIZE_ETHEREUMBLOCK, this.DEFAULT_BUFFERSIZE,direct);
				EthereumBlock eblock = ebr.readBlock();
				List<EthereumTransaction> eTrans = eblock.getEthereumTransactions();
				// validate UDFs
				EthereumTransaction transOrig0 = eTrans.get(0);
				TestEthereumTransaction trans0 = new TestEthereumTransaction();
				trans0.set(transOrig0);
				byte[] expectedHash = new byte[] {(byte)0xe2,(byte)0x7e,(byte)0x92,(byte)0x88,(byte)0xe2,(byte)0x9c,(byte)0xc8,(byte)0xeb,(byte)0x78,(byte)0xf9,(byte)0xf7,(byte)0x68,(byte)0xd8,(byte)0x9b,(byte)0xf1,(byte)0xcd,(byte)0x4b,(byte)0x68,(byte)0xb7,(byte)0x15,(byte)0xa3,(byte)0x8b,(byte)0x95,(byte)0xd4,(byte)0x6d,(byte)0x77,(byte)0x86,(byte)0x18,(byte)0xcb,(byte)0x10,(byte)0x4d,(byte)0x58};
				assertArrayEquals(expectedHash,((BytesWritable)egthUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans0)})).copyBytes(),"Block 1346406 Transaction 1 hash is correctly calculated");
				EthereumTransaction transOrig1 = eTrans.get(1);
				TestEthereumTransaction trans1 = new TestEthereumTransaction();
				trans1.set(transOrig1);
				expectedHash = new byte[] {(byte)0x7a,(byte)0x23,(byte)0x2a,(byte)0xa2,(byte)0xae,(byte)0x6a,(byte)0x5e,(byte)0x1f,(byte)0x32,(byte)0xca,(byte)0x3a,(byte)0xc9,(byte)0x3f,(byte)0x4f,(byte)0xdb,(byte)0x77,(byte)0x98,(byte)0x3e,(byte)0x93,(byte)0x2b,(byte)0x38,(byte)0x09,(byte)0x93,(byte)0x56,(byte)0x44,(byte)0x42,(byte)0x08,(byte)0xc6,(byte)0x9d,(byte)0x40,(byte)0x86,(byte)0x81};
				assertArrayEquals(expectedHash,((BytesWritable)egthUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans1)})).copyBytes(),"Block 1346406 Transaction 2 hash is correctly calculated");
				EthereumTransaction transOrig2 = eTrans.get(2);
				TestEthereumTransaction trans2 = new TestEthereumTransaction();
				trans2.set(transOrig2);
				expectedHash = new byte[] {(byte)0x14,(byte)0x33,(byte)0xe3,(byte)0xcb,(byte)0x66,(byte)0x2f,(byte)0x66,(byte)0x8d,(byte)0x87,(byte)0xb8,(byte)0x35,(byte)0x55,(byte)0x34,(byte)0x5a,(byte)0x20,(byte)0xcc,(byte)0xf8,(byte)0x70,(byte)0x6f,(byte)0x25,(byte)0x21,(byte)0x49,(byte)0x18,(byte)0xe2,(byte)0xf8,(byte)0x1f,(byte)0xe3,(byte)0xd2,(byte)0x1c,(byte)0x9d,(byte)0x5b,(byte)0x23};
				assertArrayEquals(expectedHash,((BytesWritable)egthUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans2)})).copyBytes(),"Block 1346406 Transaction 3 hash is correctly calculated");
				EthereumTransaction transOrig3 = eTrans.get(3);
				TestEthereumTransaction trans3 = new TestEthereumTransaction();
				trans3.set(transOrig3);
				expectedHash = new byte[] {(byte)0x39,(byte)0x22,(byte)0xf7,(byte)0xf6,(byte)0x0a,(byte)0x33,(byte)0xa1,(byte)0x2d,(byte)0x13,(byte)0x9d,(byte)0x67,(byte)0xfa,(byte)0x53,(byte)0x30,(byte)0xdb,(byte)0xfd,(byte)0xba,(byte)0x42,(byte)0xa4,(byte)0xb7,(byte)0x67,(byte)0x29,(byte)0x6e,(byte)0xff,(byte)0x64,(byte)0x15,(byte)0xee,(byte)0xa3,(byte)0x2d,(byte)0x8a,(byte)0x7b,(byte)0x2b};
				assertArrayEquals(expectedHash,((BytesWritable)egthUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans3)})).copyBytes(),"Block 1346406 Transaction 4 hash is correctly calculated");
				EthereumTransaction transOrig4 = eTrans.get(4);
				TestEthereumTransaction trans4 = new TestEthereumTransaction();
				trans4.set(transOrig4);
				expectedHash = new byte[] {(byte)0xbb,(byte)0x7c,(byte)0xaa,(byte)0x23,(byte)0x38,(byte)0x5a,(byte)0x0f,(byte)0x73,(byte)0x75,(byte)0x3f,(byte)0x9e,(byte)0x28,(byte)0xd8,(byte)0xf0,(byte)0x60,(byte)0x2f,(byte)0xe2,(byte)0xe7,(byte)0x2d,(byte)0x87,(byte)0xe1,(byte)0xe0,(byte)0x95,(byte)0x52,(byte)0x75,(byte)0x28,(byte)0xd1,(byte)0x44,(byte)0x88,(byte)0x5d,(byte)0x6b,(byte)0x51};
				assertArrayEquals(expectedHash,((BytesWritable)egthUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans4)})).copyBytes(),"Block 1346406 Transaction 5 hash is correctly calculated");
				EthereumTransaction transOrig5 = eTrans.get(5);
				TestEthereumTransaction trans5 = new TestEthereumTransaction();
				trans5.set(transOrig5);
				expectedHash = new byte[] {(byte)0xbc,(byte)0xde,(byte)0x6f,(byte)0x49,(byte)0x84,(byte)0x2c,(byte)0x6d,(byte)0x73,(byte)0x8d,(byte)0x64,(byte)0x32,(byte)0x8f,(byte)0x78,(byte)0x09,(byte)0xb1,(byte)0xd4,(byte)0x9b,(byte)0xf0,(byte)0xff,(byte)0x3f,(byte)0xfa,(byte)0x46,(byte)0x0f,(byte)0xdd,(byte)0xd2,(byte)0x7f,(byte)0xd4,(byte)0x2b,(byte)0x7a,(byte)0x01,(byte)0xfc,(byte)0x9a};
				assertArrayEquals(expectedHash,((BytesWritable)egthUDF.evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(trans5)})).copyBytes(),"Block 1346406 Transaction 6 hash is correctly calculated");
				    
		}
			 finally {
				if (ebr!=null) {
					ebr.close();
				}
			}

	  }
}
