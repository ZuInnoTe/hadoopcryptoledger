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
 * Hive Deserializer to represent Bitcoin Blockchain data in Hive using the BitcoinBlockFileInputFormat provided by the hadoopcryptoledger library
 */
package org.zuinnote.hadoop.bitcoin.hive.serde;

import java.io.IOException;
import java.util.*;
        

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractDeserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSerde;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

   
import org.zuinnote.hadoop.bitcoin.format.*;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

/**
* Enables access to Bitcoin Blockchain data via Hive tables. Usage:
* 
* Create table BitcoinBlockchain ROW FORMAT SERDE 'org.zuinnote.hadoop.bitcoin.hive.serde.BitcoinBlockSerde' STORED AS INPUTFORMAT 'org.zuinnote.hadoop.bitcoin.format.BitcoinBlockFileInputFormat' OUTPUTFORMAT 'org.apache.hadoop.mapreduce.lib.output.NullOutputFormat' LOCATION '/user/test/bitcoin/input';
* 
* Example structure: describe BitcoinBlockchain
*
*/

public class BitcoinBlockSerde extends AbstractDeserializer implements VectorizedSerde {

private static final Log LOG = LogFactory.getLog(BitcoinBlockSerde.class.getName());
private ObjectInspector bitcoinBlockObjectInspector;

private static final String CONF_MAXBLOCKSIZE=AbstractBitcoinRecordReader.CONF_MAXBLOCKSIZE;
private static final String CONF_FILTERMAGIC=AbstractBitcoinRecordReader.CONF_FILTERMAGIC;
private static final String CONF_USEDIRECTBUFFER=AbstractBitcoinRecordReader.CONF_USEDIRECTBUFFER;
private static final String CONF_ISSPLITABLE=AbstractBitcoinFileInputFormat.CONF_ISSPLITABLE;


/** Deserializer **/

public Object deserialize(Writable blob) {
		return blob;
}

public ObjectInspector getObjectInspector() {
	return this.bitcoinBlockObjectInspector;
}

public SerDeStats getSerDeStats() {
	// not supported
	return null;
}

public Class<? extends Writable> getSerializedClass() {
	return BitcoinBlock.class;
}

public void initialize(Configuration conf, Properties tbl) {

   // get objectinspector with introspection for class BitcoinBlockStruct to reuse functionality
    bitcoinBlockObjectInspector = ObjectInspectorFactory
        .getReflectionObjectInspector(BitcoinBlock.class,
        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
   // pass tbl properties to Configuration
	String maxBlockSizeStr=tbl.getProperty(CONF_MAXBLOCKSIZE);
	if (maxBlockSizeStr!=null) conf.setInt(CONF_MAXBLOCKSIZE, new Integer(maxBlockSizeStr).intValue());
	String filterMagicStr=tbl.getProperty(CONF_FILTERMAGIC);
	if (filterMagicStr!=null) conf.set(CONF_FILTERMAGIC, filterMagicStr);
	String useDirectBufferStr=tbl.getProperty(CONF_USEDIRECTBUFFER);
	if (useDirectBufferStr!=null) conf.setBoolean(CONF_USEDIRECTBUFFER, new Boolean(useDirectBufferStr).booleanValue());
	String isSplitableStr= tbl.getProperty(CONF_ISSPLITABLE);
	if (isSplitableStr!=null) conf.setBoolean(CONF_ISSPLITABLE, new Boolean(isSplitableStr).booleanValue());
 
}

public void initialize(Configuration conf, Properties tbl, Properties partitionProperties) {
	// currently, we do not support partitions, however, once there is a flume source to load blockchain data in realtime into HDFS, we may think about certain partitions (e.g. creating subdirectories for each month)
	initialize(conf,tbl);
	
}


/** VectorizedSerde **/

public void deserializeVector(Object rowBlob, int rowsInBlob, VectorizedRowBatch reuseBatch) throws SerDeException {
	// nothing to do here
}
       

public Writable serializeVector(VectorizedRowBatch vrg, ObjectInspector objInspector) throws SerDeException {
 throw new UnsupportedOperationException("serializeVector not supported");
}



}
