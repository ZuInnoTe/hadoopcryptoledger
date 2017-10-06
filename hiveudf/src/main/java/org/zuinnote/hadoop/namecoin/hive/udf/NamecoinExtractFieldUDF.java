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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.zuinnote.hadoop.namecoin.format.common.NamecoinUtil;

public class NamecoinExtractFieldUDF extends UDF {
	private static final Log LOG = LogFactory.getLog(NamecoinExtractFieldUDF.class.getName());
	
	/**
	 * Analyzes txOutScript (ScriptPubKey) of an output of a Namecoin Transaction to determine the key (usually the domain name or identity) and the value (more information, such as subdomains, ips etc.)
	 * 
	 * @param input BytesWritable containing a txOutScript of a Namecoin Transaction
	 * @return ArrayWritable with two entries, the first one for the key and the second one for the value
	 */
	 public ArrayList<Text> evaluate(BytesWritable input) {
		String[] nameField= NamecoinUtil.extractNamecoinField(input.copyBytes());
		Text[] nameFieldText= new Text[nameField.length];
		for (int i=0;i<nameField.length;i++) {
			nameFieldText[i]=new Text(nameField[i]);
		}
		return  new ArrayList<Text>(Arrays.asList(nameFieldText));
	 }
	 
}
