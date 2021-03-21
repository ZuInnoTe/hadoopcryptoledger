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


package org.zuinnote.spark.bitcoin.example;



import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;

import scala.Tuple2;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.zuinnote.hadoop.bitcoin.format.common.BitcoinTransaction;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlockWritable;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinTransactionWritable;

public class SparkBitcoinBlockCounterTest  {


   @BeforeAll
    public static void oneTimeSetUp() {

    }

    @AfterAll
    public static void oneTimeTearDown() {
        // one-time cleanup code
      }

    @BeforeEach
    public void setUp() {
    }

    @AfterEach
    public void tearDown() {
    }

    @Test
    public void mapNoOfTransaction() {
	SparkBitcoinBlockCounter sparkTransformator = new SparkBitcoinBlockCounter();
	  BitcoinBlockWritable testBlock = new BitcoinBlockWritable();
	  BitcoinTransaction testTransaction = new BitcoinTransaction();
	  ArrayList<BitcoinTransaction> testTransactionList = new ArrayList<BitcoinTransaction>();
	  testTransactionList.add(testTransaction);
	  testBlock.setTransactions(testTransactionList);
	  Tuple2<String,Long> result = sparkTransformator.mapNoOfTransaction(testBlock);
	  assertEquals((long)1,(long)result._2(),"One transaction should have been mapped");
    }

    @Test
    public void reduceSumUpTransactions() {
	SparkBitcoinBlockCounter sparkTransformator = new SparkBitcoinBlockCounter();
	Long transactionCountA = new Long(1);
	Long transactionCountB = new Long(2);
	assertEquals((long)3,(long)sparkTransformator.reduceSumUpTransactions(transactionCountA,transactionCountB),"Transaction count should sum up to 3");
    }


}
