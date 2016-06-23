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

package org.zuinnote.hadoop.bitcoin.hive.udf;

import org.apache.hadoop.io.BytesWritable; 
import org.apache.hadoop.io.Text; 

import org.apache.hadoop.hive.ql.exec.UDF;

import org.zuinnote.hadoop.bitcoin.format.*;

public class BitcoinScriptPaymentPatternAnalyzerUDF extends UDF {

 /**
 ** Analyzes txOutScript (ScriptPubKey) of an output of a Bitcoin Transaction to determine the payment destination
*
*/
  public Text evaluate(BytesWritable input) {
    if (input==null) return null;
    return new Text(BitcoinScriptPatternParser.getPaymentDestination(input.getBytes()));
  }
}
