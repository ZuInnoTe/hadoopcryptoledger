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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSerde;
import org.apache.hadoop.hive.serde2.AbstractDeserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Writable;
import org.zuinnote.hadoop.ethereum.format.common.EthereumBlock;
import org.zuinnote.hadoop.ethereum.format.common.EthereumBlockHeader;
import org.zuinnote.hadoop.ethereum.format.common.EthereumTransaction;
import org.zuinnote.hadoop.ethereum.format.mapred.AbstractEthereumRecordReader;
import org.zuinnote.hadoop.ethereum.hive.datatypes.HiveEthereumBlock;
import org.zuinnote.hadoop.ethereum.hive.datatypes.HiveEthereumBlockHeader;
import org.zuinnote.hadoop.ethereum.hive.datatypes.HiveEthereumTransaction;

public class EthereumBlockSerde extends AbstractDeserializer  {
	public static final String CONF_MAXBLOCKSIZE=AbstractEthereumRecordReader.CONF_MAXBLOCKSIZE;
	public static final String CONF_USEDIRECTBUFFER=AbstractEthereumRecordReader.CONF_USEDIRECTBUFFER;

	private static final Log LOG = LogFactory.getLog(EthereumBlockSerde.class.getName());
	private ObjectInspector ethereumBlockObjectInspector;
	
	@Override
	public void initialize(Configuration conf, Properties tbl) throws SerDeException {
		LOG.debug("Initializing");
		   // get objectinspector with introspection for class BitcoinBlockStruct to reuse functionality
		ethereumBlockObjectInspector = ObjectInspectorFactory
		        .getReflectionObjectInspector(HiveEthereumBlock.class,
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
		HiveEthereumBlock result=null;
		if (arg0 instanceof EthereumBlock) {
			result=convertToHiveEthereumBlock((EthereumBlock) arg0);
		}
		return result;
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
	
	
	private HiveEthereumBlock convertToHiveEthereumBlock(EthereumBlock block) {
		HiveEthereumBlockHeader ethereumBlockHeader = new HiveEthereumBlockHeader();
		ethereumBlockHeader.setParentHash(block.getEthereumBlockHeader().getParentHash());
		ethereumBlockHeader.setUncleHash(block.getEthereumBlockHeader().getUncleHash());
		ethereumBlockHeader.setCoinBase(block.getEthereumBlockHeader().getCoinBase());
		ethereumBlockHeader.setStateRoot(block.getEthereumBlockHeader().getStateRoot());
		ethereumBlockHeader.setTxTrieRoot(block.getEthereumBlockHeader().getTxTrieRoot());
		ethereumBlockHeader.setReceiptTrieRoot(block.getEthereumBlockHeader().getReceiptTrieRoot());
		ethereumBlockHeader.setLogsBloom(block.getEthereumBlockHeader().getLogsBloom());
		ethereumBlockHeader.setDifficulty(block.getEthereumBlockHeader().getDifficulty());
		ethereumBlockHeader.setTimestamp(block.getEthereumBlockHeader().getTimestamp());
		ethereumBlockHeader.setNumber(HiveDecimal.create(block.getEthereumBlockHeader().getNumber()));
		ethereumBlockHeader.setNumberRaw(block.getEthereumBlockHeader().getNumberRaw());
		ethereumBlockHeader.setGasLimit(HiveDecimal.create(block.getEthereumBlockHeader().getGasLimit()));
		ethereumBlockHeader.setGasLimitRaw(block.getEthereumBlockHeader().getGasLimitRaw());
		ethereumBlockHeader.setGasUsed(HiveDecimal.create(block.getEthereumBlockHeader().getGasUsed()));
		ethereumBlockHeader.setGasUsedRaw(block.getEthereumBlockHeader().getGasUsedRaw());
		ethereumBlockHeader.setMixHash(block.getEthereumBlockHeader().getMixHash());
		ethereumBlockHeader.setExtraData(block.getEthereumBlockHeader().getExtraData());
		ethereumBlockHeader.setNonce(block.getEthereumBlockHeader().getNonce());
		
		List<HiveEthereumTransaction> ethereumTransactions = new ArrayList<>();
		for (int i=0;i<block.getEthereumTransactions().size();i++) {
			EthereumTransaction currentTransaction = block.getEthereumTransactions().get(i);
			HiveEthereumTransaction newTransaction = new HiveEthereumTransaction();
			newTransaction.setNonce(currentTransaction.getNonce());
			newTransaction.setValue(HiveDecimal.create(currentTransaction.getValue()));
			newTransaction.setValueRaw(currentTransaction.getValueRaw());
			newTransaction.setReceiveAddress(currentTransaction.getReceiveAddress());
			newTransaction.setGasPrice(HiveDecimal.create(currentTransaction.getGasPrice()));
			newTransaction.setGasPriceRaw(currentTransaction.getGasPriceRaw());
			newTransaction.setGasLimit(HiveDecimal.create(currentTransaction.getGasLimit()));
			newTransaction.setGasLimitRaw(currentTransaction.getGasLimitRaw());
			newTransaction.setData(currentTransaction.getData());
			newTransaction.setSig_v(currentTransaction.getSig_v());
			newTransaction.setSig_r(currentTransaction.getSig_r());
			newTransaction.setSig_s(currentTransaction.getSig_s());
			ethereumTransactions.add(newTransaction);
		}
		List<HiveEthereumBlockHeader> uncleHeaders = new ArrayList<>();
		for (int i=0;i<block.getUncleHeaders().size();i++) {
			EthereumBlockHeader currentUncleHeader = block.getUncleHeaders().get(i);
			HiveEthereumBlockHeader newUncleHeader = new HiveEthereumBlockHeader();
			newUncleHeader.setParentHash(currentUncleHeader.getParentHash());
			newUncleHeader.setUncleHash(currentUncleHeader.getUncleHash());
			newUncleHeader.setCoinBase(currentUncleHeader.getCoinBase());
			newUncleHeader.setStateRoot(currentUncleHeader.getStateRoot());
			newUncleHeader.setTxTrieRoot(currentUncleHeader.getTxTrieRoot());
			newUncleHeader.setReceiptTrieRoot(currentUncleHeader.getReceiptTrieRoot());
			newUncleHeader.setLogsBloom(currentUncleHeader.getLogsBloom());
			newUncleHeader.setDifficulty(currentUncleHeader.getDifficulty());
			newUncleHeader.setTimestamp(currentUncleHeader.getTimestamp());
			newUncleHeader.setNumber(HiveDecimal.create(currentUncleHeader.getNumber()));
			newUncleHeader.setNumberRaw(currentUncleHeader.getNumberRaw());
			newUncleHeader.setGasLimit(HiveDecimal.create(currentUncleHeader.getGasLimit()));
			newUncleHeader.setGasLimitRaw(currentUncleHeader.getGasLimitRaw());
			newUncleHeader.setGasUsed(HiveDecimal.create(currentUncleHeader.getGasUsed()));
			newUncleHeader.setGasUsedRaw(currentUncleHeader.getGasUsedRaw());
			newUncleHeader.setMixHash(currentUncleHeader.getMixHash());
			newUncleHeader.setExtraData(currentUncleHeader.getExtraData());
			newUncleHeader.setNonce(currentUncleHeader.getNonce());
			uncleHeaders.add(newUncleHeader);
		}
	
		return new HiveEthereumBlock(ethereumBlockHeader,ethereumTransactions,uncleHeaders);
	}
	

}
