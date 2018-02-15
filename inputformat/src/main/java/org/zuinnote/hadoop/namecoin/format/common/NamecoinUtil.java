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
package org.zuinnote.hadoop.namecoin.format.common;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinUtil;
public class NamecoinUtil {
	private static final Log LOG = LogFactory.getLog(NamecoinUtil.class.getName());
	
	public final static byte OP_NAME_NEW=0x51;
	public final static byte OP_NAME_FIRSTUPDATE=0x52;
	public final static byte OP_NAME_UDPATE=0x53;
	public final static String STR_OP_UNKNOWN="unknown";
	public final static String STR_OP_NAME_NEW="OP_NAME_NEW";
	public final static String STR_OP_NAME_FIRSTUPDATE="OP_NAME_FIRSTUPDATE";
	public final static String STR_OP_NAME_UDPATE="OP_NAME_UPDATE";

	
	/** 
	 * Extracts a Namecoin field name (String) and value field (a JSON object) from a script. Please note that not all Namecoin transactions do contain Namecoin fields, some are coinbase (ie mining) transactions and others are regular transactions to transfer Namecoins (comparable to Bitcoin transactions=
	 * Additionally, you can extract only information related to the OPs NAME_FIRSTUPDATE and NAME_UPDATE, because NAME_NEW does not contain the name, but only a script hash that is used by NAME_FIRSTUPDATE to define the name
	 * 
	 * There are certain naming conventions that helps to identify the type of field, e.g. if name starts with:
	 * (1) d/ then it is a domain name
	 * (2) s/ or dd/ then it contains further domain data
	 * (3) id/ it contains a public online identity  
	 * 
	 * See also: 
	 * https://wiki.namecoin.org/index.php?title=Domain_Name_Specification
	 * https://wiki.namecoin.org/index.php?title=Identity
	 * 
	 * @param scriptPubKey Output script potentially containing a Namecoin operation
	 * @return Array of size 2 where the first entry is the name (e.g. d/example) and the second entry is a JSON object serialized as String, null if not a valid Namecoin DNS field
	 */
	public static String[] extractNamecoinField(byte[] scriptPubKey) {
		// check if valid script
		if ((scriptPubKey==null) || (scriptPubKey.length<2)) {
			return null;
		}
		// only firstupdate and update work
		    if (!((scriptPubKey[0]==NamecoinUtil.OP_NAME_UDPATE)|| (scriptPubKey[0]==NamecoinUtil.OP_NAME_FIRSTUPDATE))) {
		    		return null;
		    }
		    String[] result = new String[2];
		    // convert script into ByteBuffer
		    ByteBuffer scriptByteBuffer = ByteBuffer.wrap(scriptPubKey);
		    // skip op
		    scriptByteBuffer.get();
		    // read name
		    		// get size
		    		long nameSize=BitcoinUtil.convertVarIntByteBufferToLong(scriptByteBuffer);
		    		// extract name
		    		byte[] nameByteArray = new byte[(int)nameSize];
		    		scriptByteBuffer.get(nameByteArray);
		    		String name = new String(nameByteArray,Charset.forName("UTF-8"));
		    		result[0]=name;
		    	if (scriptPubKey[0]==NamecoinUtil.OP_NAME_FIRSTUPDATE) {
		    	// skip intermediate information
		    		long intermediateInformationSize = BitcoinUtil.convertVarIntByteBufferToLong(scriptByteBuffer);
		    		byte[] intermediateInformation=new byte[(int)intermediateInformationSize];
		    		scriptByteBuffer.get(intermediateInformation);
		    	}
		    // read value
		    		long valueSize = BitcoinUtil.convertVarIntByteBufferToLong(scriptByteBuffer);
		    		byte[] valueByteArray = new byte[(int)valueSize];
		    		scriptByteBuffer.get(valueByteArray);
		    		String value = new String (valueByteArray, Charset.forName("UTF-8"));
		    		result[1]=value;
			return result;
	}
	
	
	/**
	 * Determines the name operation (if any) of the given script. Please note that not all Namecoin transactions do contain Namecoin fields, some are coinbase (ie mining) transactions and others are regular transactions to transfer Namecoins (comparable to Bitcoin transactions
	 * 
	 * @param scriptPubKey Output script potentially containing a Namecoin operation
	 * @return  Name operation: "OP_NAME_NEW", "OP_NAME_FIRST_UPDATE", "OP_NAME_UPDATE" or in case it cannot be determined: "unknown"
	 */
	public static String getNameOperation(byte[] scriptPubKey) {
		if (scriptPubKey==null) {
			return NamecoinUtil.STR_OP_UNKNOWN;
		}
		if (scriptPubKey.length>1)  {
			byte nameOp=scriptPubKey[0];
			switch(nameOp) {
				case NamecoinUtil.OP_NAME_NEW: 
					return NamecoinUtil.STR_OP_NAME_NEW;
				case NamecoinUtil.OP_NAME_FIRSTUPDATE: 
					return NamecoinUtil.STR_OP_NAME_FIRSTUPDATE;
				case NamecoinUtil.OP_NAME_UDPATE: 
					return NamecoinUtil.STR_OP_NAME_UDPATE;
				default:
						return NamecoinUtil.STR_OP_UNKNOWN;
			}

		}
		// in all other cases we do not know
		return NamecoinUtil.STR_OP_UNKNOWN;
	}
	
	

}
