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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;

import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinUtil;
import org.zuinnote.hadoop.namecoin.format.common.NamecoinUtil;

/**
 * @author jornfranke
 *
 */
public class NamecoinUDFTest {

	@Test
	public void extractNamecoinFieldFirstUpdate() {
		String firstUpdateScript ="520A642F666C6173687570641460C7B068EDEA60281DAF424C38D8DAB87C96CF993D7B226970223A223134352E3234392E3130362E323238222C226D6170223A7B222A223A7B226970223A223134352E3234392E3130362E323238227D7D7D6D6D76A91451B4FC93AAB8CBDBD0AC9BC8EAF824643FC1E29B88AC";
		byte[] firstUpdateScriptBytes = BitcoinUtil.convertHexStringToByteArray(firstUpdateScript);
		NamecoinExtractFieldUDF nefu = new NamecoinExtractFieldUDF();
		ArrayList<String> resultList = nefu.evaluate(new BytesWritable(firstUpdateScriptBytes));
		String[] result=resultList.toArray(new String[resultList.size()]);
		assertNotNull("Valid result obtained", result);
		// test for domain name
		assertEquals("Domain name of first update detected correctly","d/flashupd",result[0]);
		// test for domain value
		assertEquals("Domain value of first update detected correctly","{\"ip\":\"145.249.106.228\",\"map\":{\"*\":{\"ip\":\"145.249.106.228\"}}}",result[1]);
		
	}
	
	
	@Test
	public void extractNamecoinFieldUpdate() {
		String updateScript = "5309642F70616E656C6B612D7B226970223A22382E382E382E38222C226D6170223A7B222A223A7B226970223A22382E382E382E38227D7D7D6D7576A9148D804B079AC79AD0CA108A4E5B679DB591FF069B88AC";
		byte[] updateScriptBytes = BitcoinUtil.convertHexStringToByteArray(updateScript);
		NamecoinExtractFieldUDF nefu = new NamecoinExtractFieldUDF();
		ArrayList<String> resultList = nefu.evaluate(new BytesWritable(updateScriptBytes));
		String[] result=resultList.toArray(new String[resultList.size()]);
		assertNotNull("Valid result obtained", result);
		// test for domain name
		assertEquals("Domain name of first update detected correctly","d/panelka",result[0]);
		// test for domain value
		assertEquals("Domain value of first update detected correctly","{\"ip\":\"8.8.8.8\",\"map\":{\"*\":{\"ip\":\"8.8.8.8\"}}}",result[1]);
		
	}
	
	
	@Test
	public void getNameOperationUDF() {
		NamecoinGetNameOperationUDF ngno = new NamecoinGetNameOperationUDF();
		// new
		String newScript = "511459C39A7CC5E0B91801294A272AD558B1F67A4E6D6D76A914DD900A6C1223698FC262E28C8A1D8D73B40B375188AC";
		byte[] newScriptByte = BitcoinUtil.convertHexStringToByteArray(newScript);
		String resultOpNew = ngno.evaluate(new BytesWritable(newScriptByte)).toString(); 
		assertEquals("Script containing new op detected correctly",NamecoinUtil.STR_OP_NAME_NEW,resultOpNew);
		// firstupdate
		String firstUpdateScript ="520A642F666C6173687570641460C7B068EDEA60281DAF424C38D8DAB87C96CF993D7B226970223A223134352E3234392E3130362E323238222C226D6170223A7B222A223A7B226970223A223134352E3234392E3130362E323238227D7D7D6D6D76A91451B4FC93AAB8CBDBD0AC9BC8EAF824643FC1E29B88AC";
		byte[] firstUpdateScriptByte = BitcoinUtil.convertHexStringToByteArray(firstUpdateScript);
		String resultOpFirstUpdate=ngno.evaluate(new BytesWritable(firstUpdateScriptByte)).toString(); 
		assertEquals("Script containing firstupdate op detected correctly",NamecoinUtil.STR_OP_NAME_FIRSTUPDATE,resultOpFirstUpdate);
		// update
		String updateScript = "5309642F70616E656C6B612D7B226970223A22382E382E382E38222C226D6170223A7B222A223A7B226970223A22382E382E382E38227D7D7D6D7576A9148D804B079AC79AD0CA108A4E5B679DB591FF069B88AC";
		byte[] updateScriptByte = BitcoinUtil.convertHexStringToByteArray(updateScript);
		String resultOpUpdate=ngno.evaluate(new BytesWritable(updateScriptByte)).toString(); 
		assertEquals("Script containing updateScript op detected correctly",NamecoinUtil.STR_OP_NAME_UDPATE,resultOpUpdate);
	}
	
		
	
}
