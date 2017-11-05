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
package org.zuinnote.hadoop.ethereum.hive.serde;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSerde;
import org.apache.hadoop.hive.serde2.AbstractDeserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Writable;
import org.zuinnote.hadoop.ethereum.format.common.EthereumBlock;
import org.zuinnote.hadoop.ethereum.format.mapred.AbstractEthereumRecordReader;

public class EthereumBlockSerde extends AbstractDeserializer implements VectorizedSerde {
	public static final String CONF_MAXBLOCKSIZE=AbstractEthereumRecordReader.CONF_MAXBLOCKSIZE;
	public static final String CONF_USEDIRECTBUFFER=AbstractEthereumRecordReader.CONF_USEDIRECTBUFFER;

	private static final Log LOG = LogFactory.getLog(EthereumBlockSerde.class.getName());
	private ObjectInspector ethereumBlockObjectInspector;
	
	@Override
	public void initialize(Configuration conf, Properties tbl) throws SerDeException {
		LOG.debug("Initializing");
		   // get objectinspector with introspection for class BitcoinBlockStruct to reuse functionality
		ethereumBlockObjectInspector = ObjectInspectorFactory
		        .getReflectionObjectInspector(EthereumBlock.class,
		        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		   // pass tbl properties to Configuration
			String maxBlockSizeStr=tbl.getProperty(EthereumBlockSerde.CONF_MAXBLOCKSIZE);
			if (maxBlockSizeStr!=null) {
				 conf.setInt(EthereumBlockSerde.CONF_MAXBLOCKSIZE, Integer.parseInt(maxBlockSizeStr));
				 LOG.info("Setting max block size: "+maxBlockSizeStr);
			}
			
			String useDirectBufferStr=tbl.getProperty(EthereumBlockSerde.CONF_USEDIRECTBUFFER);
			if (useDirectBufferStr!=null) {
				conf.setBoolean(EthereumBlockSerde.CONF_USEDIRECTBUFFER, Boolean.parseBoolean(useDirectBufferStr));
				LOG.info("Use direct buffer: "+useDirectBufferStr);
			}
			LOG.debug("Finish initializion");
		 
		
	}

	@Override
	public Object deserialize(Writable arg0) throws SerDeException {
		return arg0;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return this.ethereumBlockObjectInspector;
	}

	@Override
	public SerDeStats getSerDeStats() {
		// not supported
		return null;
	}
	
	@Override
	public Writable serializeVector(VectorizedRowBatch vrg, ObjectInspector objInspector) throws SerDeException {
		 throw new UnsupportedOperationException("serializeVector not supported");
	}

	@Override
	public void deserializeVector(Object rowBlob, int rowsInBlob, VectorizedRowBatch reuseBatch) throws SerDeException {
		// nothing to do here
		
	}

}
