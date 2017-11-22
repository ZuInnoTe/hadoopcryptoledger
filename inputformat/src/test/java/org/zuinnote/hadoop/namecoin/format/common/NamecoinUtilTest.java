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


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinUtil;

public class NamecoinUtilTest {
	
	
	@Test
	public void extractNamecoinFieldFirstUpdate() {
		String firstUpdateScript ="520A642F666C6173687570641460C7B068EDEA60281DAF424C38D8DAB87C96CF993D7B226970223A223134352E3234392E3130362E323238222C226D6170223A7B222A223A7B226970223A223134352E3234392E3130362E323238227D7D7D6D6D76A91451B4FC93AAB8CBDBD0AC9BC8EAF824643FC1E29B88AC";
		String[] result=NamecoinUtil.extractNamecoinField(BitcoinUtil.convertHexStringToByteArray(firstUpdateScript));
		assertNotNull( result,"Valid result obtained");
		// test for domain name
		assertEquals("d/flashupd",result[0],"Domain name of first update detected correctly");
		// test for domain value
		assertEquals("{\"ip\":\"145.249.106.228\",\"map\":{\"*\":{\"ip\":\"145.249.106.228\"}}}",result[1],"Domain value of first update detected correctly");
		
	}
	
	
	@Test
	public void extractNamecoinFieldUpdate() {
		String updateScript = "5309642F70616E656C6B612D7B226970223A22382E382E382E38222C226D6170223A7B222A223A7B226970223A22382E382E382E38227D7D7D6D7576A9148D804B079AC79AD0CA108A4E5B679DB591FF069B88AC";
		String[] result=NamecoinUtil.extractNamecoinField(BitcoinUtil.convertHexStringToByteArray(updateScript));
		assertNotNull( result,"Valid result obtained");
		// test for domain name
		assertEquals("d/panelka",result[0],"Domain name of first update detected correctly");
		// test for domain value
		assertEquals("{\"ip\":\"8.8.8.8\",\"map\":{\"*\":{\"ip\":\"8.8.8.8\"}}}",result[1],"Domain value of first update detected correctly");
		
	}
	
	@Test
	public void extractNamecoinFieldInvalid() {
		String[] resultNull = NamecoinUtil.extractNamecoinField(null);
		assertNull(resultNull,"Null script leads to null result");
		String[] resultInvalid = NamecoinUtil.extractNamecoinField(new byte[] {0x01,0x02,0x03});
		assertNull(resultInvalid,"Invalid script leads to null result");
	}
	
	@Test
	public void getNameOperationPositive() {
		// new
		String newScript = "511459C39A7CC5E0B91801294A272AD558B1F67A4E6D6D76A914DD900A6C1223698FC262E28C8A1D8D73B40B375188AC";
		String resultOpNew=NamecoinUtil.getNameOperation(BitcoinUtil.convertHexStringToByteArray(newScript));
		assertEquals(NamecoinUtil.STR_OP_NAME_NEW,resultOpNew,"Script containing new op detected correctly");
		// firstupdate
		String firstUpdateScript ="520A642F666C6173687570641460C7B068EDEA60281DAF424C38D8DAB87C96CF993D7B226970223A223134352E3234392E3130362E323238222C226D6170223A7B222A223A7B226970223A223134352E3234392E3130362E323238227D7D7D6D6D76A91451B4FC93AAB8CBDBD0AC9BC8EAF824643FC1E29B88AC";
		String resultOpFirstUpdate=NamecoinUtil.getNameOperation(BitcoinUtil.convertHexStringToByteArray(firstUpdateScript));
		assertEquals(NamecoinUtil.STR_OP_NAME_FIRSTUPDATE,resultOpFirstUpdate,"Script containing firstupdate op detected correctly");
		// update
		String updateScript = "5309642F70616E656C6B612D7B226970223A22382E382E382E38222C226D6170223A7B222A223A7B226970223A22382E382E382E38227D7D7D6D7576A9148D804B079AC79AD0CA108A4E5B679DB591FF069B88AC";
		String resultOpUpdate=NamecoinUtil.getNameOperation(BitcoinUtil.convertHexStringToByteArray(updateScript));
		assertEquals(NamecoinUtil.STR_OP_NAME_UDPATE,resultOpUpdate,"Script containing updateScript op detected correctly");
	}
	
	@Test
	public void getNameOperationNegative() {
		// null
		assertEquals(NamecoinUtil.getNameOperation(null),NamecoinUtil.STR_OP_UNKNOWN,"Script containing null correctly");
		// zero bytes
		assertEquals(NamecoinUtil.getNameOperation(new byte[0]),NamecoinUtil.STR_OP_UNKNOWN,"Script containing byte array zero length correctly");
		// two byte not namecoin op
		assertEquals(NamecoinUtil.getNameOperation(new byte[] {0x01,0x02}),NamecoinUtil.STR_OP_UNKNOWN,"Script containing no namecoin op correctly");
	}
	
	


}
