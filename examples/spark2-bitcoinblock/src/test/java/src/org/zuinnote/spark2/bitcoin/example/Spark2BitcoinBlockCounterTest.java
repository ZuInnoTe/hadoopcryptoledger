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

/**
 * Simple Driver for a Spark 2 job counting the number of transactons in a given block from the specified files containing Bitcoin blockchain data
 */
package org.zuinnote.spark2.bitcoin.example;


import static org.junit.Assert.assertEquals;



import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.After;

import java.util.ArrayList;

import scala.Tuple2;

import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinTransaction;

public class Spark2BitcoinBlockCounterTest  {


   @BeforeClass
    public static void oneTimeSetUp() {
     
    }

    @AfterClass
    public static void oneTimeTearDown() {
        // one-time cleanup code
      }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void mapNoOfTransaction() {
	Spark2BitcoinBlockCounter sparkTransformator = new Spark2BitcoinBlockCounter();
	  BitcoinBlock testBlock = new BitcoinBlock();
	  BitcoinTransaction testTransaction = new BitcoinTransaction();
	  ArrayList<BitcoinTransaction> testTransactionList = new ArrayList<BitcoinTransaction>();
	  testTransactionList.add(testTransaction);
	  testBlock.setTransactions(testTransactionList);
	  Tuple2<String,Long> result = sparkTransformator.mapNoOfTransaction(testBlock);
	  assertEquals("One transaction should have been mapped",(long)1,(long)result._2());
    }

    @Test
    public void reduceSumUpTransactions() {
	Spark2BitcoinBlockCounter sparkTransformator = new Spark2BitcoinBlockCounter();
	Long transactionCountA = new Long(1);
	Long transactionCountB = new Long(2);
	assertEquals("Transaction count should sum up to 3",(long)3,(long)sparkTransformator.reduceSumUpTransactions(transactionCountA,transactionCountB));
    }
    

}
