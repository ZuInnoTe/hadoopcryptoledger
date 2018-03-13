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
import org.zuinnote.hadoop.ethereum.format.common.EthereumTransaction;
import org.zuinnote.hadoop.ethereum.hive.datatypes.HiveEthereumTransaction;

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
		if (row instanceof HiveEthereumTransaction) { // this is the case if you use the EthereumBlockSerde
			result=EthereumUDFUtil.convertToEthereumTransaction((HiveEthereumTransaction) row);
		} else { // this is the case if you have imported the EthereumTransaction in any other Hive supported format, such as ORC or Parquet
			StructField nonceSF=soi.getStructFieldRef("nonce");
			StructField valueSF=soi.getStructFieldRef("valueRaw");
			StructField receiveAddressSF=soi.getStructFieldRef("receiveAddress");
			StructField gasPriceSF=soi.getStructFieldRef("gasPriceRaw");
			StructField gasLimitSF=soi.getStructFieldRef("gasLimitRaw");
			StructField dataSF=soi.getStructFieldRef("data");
			StructField sigVSF=soi.getStructFieldRef("sig_v");
			StructField sigRSF=soi.getStructFieldRef("sig_r");
			StructField sigSSF=soi.getStructFieldRef("sig_s");
			boolean baseNull =  (nonceSF==null) || (valueSF==null)|| (receiveAddressSF==null);
			boolean gasDataNull = (gasPriceSF==null) || (gasLimitSF==null)|| (dataSF==null);
			boolean sigNull = (sigVSF==null) || (sigRSF==null)|| (sigSSF==null);
			if (baseNull || gasDataNull || sigNull) {
				LOG.error("Structure does not correspond to EthereumTransaction. You need the fields nonce, valueRaw, reciveAddress, gasPriceRaw, gasLimitRaw, data, sig_v, sig_r, sig_s");
				return null;
			}
			byte[] nonce = (byte[]) soi.getStructFieldData(row,nonceSF);
			byte[] valueRaw = (byte[]) soi.getStructFieldData(row,valueSF);
			byte[] receiveAddress = (byte[]) soi.getStructFieldData(row,receiveAddressSF);
			byte[] gasPriceRaw =(byte[]) soi.getStructFieldData(row,gasPriceSF);
			byte[] gasLimitRaw = (byte[]) soi.getStructFieldData(row,gasLimitSF);
			byte[] data = (byte[]) soi.getStructFieldData(row,dataSF);
			byte[] sig_v = (byte[]) soi.getStructFieldData(row,sigVSF);
			byte[] sig_r = (byte[]) soi.getStructFieldData(row,sigRSF);
			byte[] sig_s = (byte[]) soi.getStructFieldData(row,sigSSF);
			result=new EthereumTransaction();
			result.setNonce(nonce);
			result.setValueRaw(valueRaw);
			result.setReceiveAddress(receiveAddress);
			result.setGasPriceRaw(gasPriceRaw);
			result.setGasLimitRaw(gasLimitRaw);
			result.setData(data);
			result.setSig_v(sig_v);
			result.setSig_r(sig_r);
			result.setSig_s(sig_s);
		}
		return result;
	}
	
	
	/** 
	 * Convert HiveEthereumTransaction to Ethereum Transaction
	 * 
	 * @param transaction in HiveEthereumTransaction format
	 * @return transaction in EthereumTransactionFormat
	 */
	public static EthereumTransaction convertToEthereumTransaction(HiveEthereumTransaction transaction) {
			EthereumTransaction result = new EthereumTransaction();
			// note we set only the raw values, the other ones can be automatically dervied by the EthereumTransaction class if needed. This avoids conversion issues for large numbers
			result.setNonce(transaction.getNonce());
			result.setValueRaw(transaction.getValueRaw());
			result.setReceiveAddress(transaction.getReceiveAddress());
			result.setGasPriceRaw(transaction.getGasPriceRaw());
			result.setGasLimitRaw(transaction.getGasLimitRaw());
			result.setData(transaction.getData());
			result.setSig_v(transaction.getSig_v());
			result.setSig_r(transaction.getSig_r());
			result.setSig_s(transaction.getSig_s());
			return result;
	}

}
