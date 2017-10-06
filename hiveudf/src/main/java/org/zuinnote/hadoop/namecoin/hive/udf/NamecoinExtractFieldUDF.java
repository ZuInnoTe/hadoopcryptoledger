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

package org.zuinnote.hadoop.namecoin.hive.udf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinTransaction;
import org.zuinnote.hadoop.namecoin.format.common.NamecoinUtil;

public class NamecoinExtractFieldUDF extends GenericUDF {
	private static final Log LOG = LogFactory.getLog(NamecoinExtractFieldUDF.class.getName());
	private WritableBinaryObjectInspector wboi;

	 
	 /**
	 *
	 * Initialize HiveUDF and create object inspectors. It requires that the argument length is = 1 and that the ObjectInspector of arguments[0] is of type org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector
	 *
	 * @param arguments array of length 1 containing one WritableBinaryObjectInspector
	 *
	 * @return ObjectInspector that is able to parse the result of the evaluate method of the UDF (List Object Inspector for Strings)
	 *
	 * @throws org.apache.hadoop.hive.ql.exec.UDFArgumentException in case the first argument is not of WritableBinaryObjectInspector
	 * @throws org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException in case the number of arguments is != 1
	 *
	 */
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments==null) {
	      		throw new UDFArgumentLengthException("namecoinExtractField only takes one argument: Binary ");
		}
	  	if (arguments.length != 1) {
	      		throw new UDFArgumentLengthException("namecoinExtractField only takes one argument: Binary ");
		}
		if (!(arguments[0] instanceof WritableBinaryObjectInspector)) { 
			throw new UDFArgumentException("first argument must be a Binary containing a Namecoin script");
		}
		// these are only used for bitcointransaction structs exported to other formats, such as ORC
		this.wboi = (WritableBinaryObjectInspector)arguments[0];;
		// the UDF returns the hash value of a BitcoinTransaction as byte array
		return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
	}

	/**
	 * 
	 *  Analyzes txOutScript (ScriptPubKey) of an output of a Namecoin Transaction to determine the key (usually the domain name or identity) and the value (more information, such as subdomains, ips etc.)
		* 
		* @param arguments array of length 1 containing one object of type Binary representing the txOutScript of a Namecoin firstupdate or update operation
		*
		* @return Array (List) of size 2, where the first item is the domain name and the second item contains more information, such as subdomains, in JSON format
		*
		* @throws org.apache.hadoop.hive.ql.metadata.HiveException in case an internal HiveError occurred
	 */
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		if ((arguments==null) || (arguments.length!=1)) { 
			return null;
		}
		BytesWritable txOutScriptBW = (BytesWritable)arguments[0].get();
		String[] nameField= NamecoinUtil.extractNamecoinField(txOutScriptBW.copyBytes());
		Text[] nameFieldText= new Text[nameField.length];
		for (int i=0;i<nameField.length;i++) {
			nameFieldText[i]=new Text(nameField[i]);
		}
		return  new ArrayList<Text>(Arrays.asList(nameFieldText));
	}

	/**
	 * 
	 * @param children
	 * @return
	 */
	@Override
	public String getDisplayString(String[] children) {
		return "hclNamecoinExtractField()";
	}
	 
}
