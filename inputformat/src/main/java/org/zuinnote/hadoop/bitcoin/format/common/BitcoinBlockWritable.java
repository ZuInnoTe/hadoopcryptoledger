/**
* Copyright 2021 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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

package org.zuinnote.hadoop.bitcoin.format.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;


/**
* Dedicated class for Hadoop-based applications
*/
public class BitcoinBlockWritable extends BitcoinBlock implements Writable {
	/** Writable **/

	  @Override
	  public void write(DataOutput dataOutput) throws IOException {
	    throw new UnsupportedOperationException("write unsupported");
	  }

	  @Override
	  public void readFields(DataInput dataInput) throws IOException {
	    throw new UnsupportedOperationException("readFields unsupported");
	  }

}
