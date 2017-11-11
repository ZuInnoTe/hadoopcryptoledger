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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.zuinnote.hadoop.ethereum.format.common.EthereumTransaction;
import org.zuinnote.hadoop.ethereum.format.common.EthereumUtil;

/**
 * Utility class for Hive UDFs for Ethereum
 *
 */
public class EthereumUDFUtil {
	private static final Log LOG = LogFactory.getLog(EthereumUDFUtil.class.getName());
	private StructObjectInspector soi;
	
	
	/**
	 * 
	 * 
	 * @param soi ObjectInspector provided by Hive UDF
	 */
	public EthereumUDFUtil(StructObjectInspector soi) {
		this.soi = soi;
	}
	
	/**
	 * Create an object of type EthereumTransaction from any HiveTable
	 * 
	 * @param row Object
	 * @return EthereumTransaction corresponding to object
	 */
	public  EthereumTransaction getEthereumTransactionFromObject(Object row) {
		EthereumTransaction result=null;
		if (row instanceof EthereumTransaction) { // this is the case if you use the EthereumBlockSerde
			result=(EthereumTransaction) row;
		} else { // this is the case if you have imported the EthereumTransaction in any other Hive supported format, such as ORC or Parquet
			StructField nonceSF=soi.getStructFieldRef("nonce");
			StructField valueSF=soi.getStructFieldRef("value");
			StructField receiveAddressSF=soi.getStructFieldRef("receiveAddress");
			StructField gasPriceSF=soi.getStructFieldRef("gasPrice");
			StructField gasLimitSF=soi.getStructFieldRef("gasLimit");
			StructField dataSF=soi.getStructFieldRef("data");
			StructField sigVSF=soi.getStructFieldRef("sig_v");
			StructField sigRSF=soi.getStructFieldRef("sig_r");
			StructField sigSSF=soi.getStructFieldRef("sig_s");
			boolean baseNull =  (nonceSF==null) || (valueSF==null)|| (receiveAddressSF==null);
			boolean gasDataNull = (gasPriceSF==null) || (gasLimitSF==null)|| (dataSF==null);
			boolean sigNull = (sigVSF==null) || (sigRSF==null)|| (sigSSF==null);
			if (baseNull || gasDataNull || sigNull) {
				LOG.warn("Structure does not correspond to EthereumTransaction");
				return null;
			}
			byte[] nonce = (byte[]) soi.getStructFieldData(row,nonceSF);
			long value = (long) soi.getStructFieldData(row,valueSF);
			byte[] receiveAddress = (byte[]) soi.getStructFieldData(row,receiveAddressSF);
			long gasPrice =(long) soi.getStructFieldData(row,gasPriceSF);
			long gasLimit = (long) soi.getStructFieldData(row,gasLimitSF);
			byte[] data = (byte[]) soi.getStructFieldData(row,dataSF);
			byte[] sig_v = (byte[]) soi.getStructFieldData(row,sigVSF);
			byte[] sig_r = (byte[]) soi.getStructFieldData(row,sigRSF);
			byte[] sig_s = (byte[]) soi.getStructFieldData(row,sigSSF);
			result=new EthereumTransaction();
			result.setNonce(nonce);
			result.setValue(value);
			result.setReceiveAddress(receiveAddress);
			result.setGasPrice(gasPrice);
			result.setGasLimit(gasLimit);
			result.setData(data);
			result.setSig_v(sig_v);
			result.setSig_r(sig_r);
			result.setSig_s(sig_s);
		}
		return result;
	}

}
