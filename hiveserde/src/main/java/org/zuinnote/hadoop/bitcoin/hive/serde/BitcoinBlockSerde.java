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

import java.util.Properties;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.serde2.AbstractDeserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSerde;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

   
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock;
import org.zuinnote.hadoop.bitcoin.format.mapred.AbstractBitcoinRecordReader;

import org.zuinnote.hadoop.bitcoin.format.mapred.AbstractBitcoinFileInputFormat;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

/**
* Enables access to Bitcoin Blockchain data via Hive tables. Usage:
* 
* Create table BitcoinBlockchain ROW FORMAT SERDE 'org.zuinnote.hadoop.bitcoin.hive.serde.BitcoinBlockSerde' STORED AS INPUTFORMAT 'org.zuinnote.hadoop.bitcoin.format.mapred.BitcoinBlockFileInputFormat' OUTPUTFORMAT 'org.apache.hadoop.mapred.lib.NullOutputFormat' LOCATION '/user/test/bitcoin/input';
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
@Override
public Object deserialize(Writable blob) {
		return blob;
}

@Override
public ObjectInspector getObjectInspector() {
	return this.bitcoinBlockObjectInspector;
}

@Override
public SerDeStats getSerDeStats() {
	// not supported
	return null;
}

public Class<? extends Writable> getSerializedClass() {
	return BitcoinBlock.class;
}

@Override
public void initialize(Configuration conf, Properties tbl) {
	LOG.debug("Initializing BitcoinBlockSerde");
   // get objectinspector with introspection for class BitcoinBlockStruct to reuse functionality
    bitcoinBlockObjectInspector = ObjectInspectorFactory
        .getReflectionObjectInspector(BitcoinBlock.class,
        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
   // pass tbl properties to Configuration
	String maxBlockSizeStr=tbl.getProperty(BitcoinBlockSerde.CONF_MAXBLOCKSIZE);
	if (maxBlockSizeStr!=null) {
		 conf.setInt(BitcoinBlockSerde.CONF_MAXBLOCKSIZE, Integer.parseInt(maxBlockSizeStr));
		 LOG.info("Setting max block size: "+maxBlockSizeStr);
	}
	String filterMagicStr=tbl.getProperty(BitcoinBlockSerde.CONF_FILTERMAGIC);
	if (filterMagicStr!=null) {
		 conf.set(BitcoinBlockSerde.CONF_FILTERMAGIC, filterMagicStr);
		 LOG.info("Setting filter magic: "+filterMagicStr);
	}
	String useDirectBufferStr=tbl.getProperty(BitcoinBlockSerde.CONF_USEDIRECTBUFFER);
	if (useDirectBufferStr!=null) {
		conf.setBoolean(BitcoinBlockSerde.CONF_USEDIRECTBUFFER, Boolean.parseBoolean(useDirectBufferStr));
		LOG.info("Use direct buffer: "+useDirectBufferStr);
	}
	String isSplitableStr= tbl.getProperty(CONF_ISSPLITABLE);
	if (isSplitableStr!=null) {
		conf.setBoolean(BitcoinBlockSerde.CONF_ISSPLITABLE, Boolean.parseBoolean(isSplitableStr));
		LOG.info("Enable splitable heuristic: "+isSplitableStr);
	}
	LOG.debug("Finish initializion BitcoinBlockSerde");
 
}



/** VectorizedSerde **/
@Override
public void deserializeVector(Object rowBlob, int rowsInBlob, VectorizedRowBatch reuseBatch) throws SerDeException {
	// nothing to do here
}
       
@Override
public Writable serializeVector(VectorizedRowBatch vrg, ObjectInspector objInspector) throws SerDeException {
 throw new UnsupportedOperationException("serializeVector not supported");
}



}
