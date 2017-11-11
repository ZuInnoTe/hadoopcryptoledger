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


package org.zuinnote.hadoop.ethereum.example;


import static org.junit.Assert.assertEquals;



import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.After;

import mockit.*;

import java.lang.InterruptedException;
import java.io.IOException;

import java.util.ArrayList;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.zuinnote.hadoop.ethereum.format.common.EthereumBlock;
import org.zuinnote.hadoop.ethereum.format.common.EthereumTransaction;

import org.zuinnote.hadoop.ethereum.example.tasks.EthereumBlockMap;
import org.zuinnote.hadoop.ethereum.example.tasks.EthereumBlockReducer;

public final class MapReduceEthereumBlockTest {


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
    public void map(@Mocked final Mapper.Context defaultContext) throws IOException,InterruptedException {
	EthereumBlockMap mapper = new EthereumBlockMap();
	final BytesWritable key = new BytesWritable( );
	final EthereumBlock value = new EthereumBlock(null,new ArrayList<EthereumTransaction>(),null);
	final Text defaultKey = new Text("Transaction Count:");
	final IntWritable nullInt = new IntWritable(0);
	new Expectations() {{
		defaultContext.write(defaultKey,nullInt); times=1;
	}};
	mapper.map(key,value,defaultContext);
    }

    @Test
    public void reduce(@Mocked final Reducer.Context defaultContext) throws IOException,InterruptedException {
	EthereumBlockReducer reducer = new EthereumBlockReducer();
	final Text defaultKey = new Text("Transaction Count:");
	final IntWritable oneInt = new IntWritable(1);
	final IntWritable twoInt = new IntWritable(2);
	final LongWritable resultLong = new LongWritable(3);
	final ArrayList al = new ArrayList<IntWritable>();
	al.add(oneInt);
	al.add(twoInt);
	new Expectations() {{
		defaultContext.write(defaultKey,resultLong); times=1;
	}};
	reducer.reduce(defaultKey,al,defaultContext);
    }

       

}
