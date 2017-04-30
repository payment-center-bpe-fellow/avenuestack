package com.sdg.avenuestack.impl.avenue;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import avenuestack.impl.avenue.ByteBufferWithReturnCode;
import avenuestack.impl.avenue.MapWithReturnCode;
import avenuestack.impl.avenue.TlvCodec;
import avenuestack.impl.avenue.TlvCodecs;

public class TlvCodecsTest {

	@Test
	public void testA() throws Exception {
		TlvCodecs codecs = new TlvCodecs("src/test/resources/avenue_conf");
		TlvCodec c = codecs.findTlvCodec(451);
		assertNotNull(c);
		String id = codecs.serviceNameToId("a.b");
		assertNotNull("0.0");
		id = codecs.serviceNameToId("configservice.cdnkey_detail");
		assertEquals("451.100",id);
		id = codecs.serviceNameToId("configservice.channel_list");
		assertEquals("451.110",id);
		id = codecs.serviceNameToId("configservice.CHannel_list");
		assertEquals("451.110",id);
		
		String name = codecs.serviceIdToName(451,110);
		assertEquals("configservice.channel_list",name);
		name = codecs.serviceIdToName(111,110);
		assertEquals("service111.msg110",name);
		name = codecs.serviceIdToName(451,999);
		assertEquals("configservice.msg999",name);
	
		List<Integer> serviceIds = codecs.allServiceIds();
		assertTrue(serviceIds.contains(451));
		assertTrue(serviceIds.contains(4510001));
	}

	@Test
	public void testB() throws Exception {
		TlvCodecs codecs = new TlvCodecs("src/test/resources/avenue_conf");
		TlvCodec c = codecs.findTlvCodec(451);
		
		HashMap<String,Object> map = new HashMap<String,Object>();
		
		ByteBufferWithReturnCode ret = c.encodeRequest(100,map,1);
		assertNotNull(ret);
		assertNotNull(ret.bb);
		assertEquals(0,ret.ec);
		MapWithReturnCode ret2 = c.decodeRequest(100,ret.bb,1);
		assertNotNull(ret2);
		assertNotNull(ret2.body);
		assertEquals(0,ret2.ec);
		assertEquals(0,ret2.body.size());
		
		map.clear();
		map.put("cur_source", "1"); // string
		map.put("cur_userid", "1001");
		map.put("cur_username", "TEST001");
		map.put("cur_proj_id", "100");
		
		ret = c.encodeRequest(100,map,1);
		assertNotNull(ret);
		ret2 = c.decodeRequest(100,ret.bb,1);
		assertEquals("1",ret2.body.get("cur_source"));
		assertEquals("1001",ret2.body.get("cur_userid"));
		assertEquals("TEST001",ret2.body.get("cur_username"));
		assertEquals("100",ret2.body.get("cur_proj_id"));
		assertEquals("1",ret2.body.get("cur_source"));

		map.clear();
		map.put("cur_source", 2); // integer
		ret = c.encodeRequest(100,map,1);
		assertNotNull(ret);
		ret2 = c.decodeRequest(100,ret.bb,1);
		assertEquals("2",ret2.body.get("cur_source"));

		map.clear();
		map.put("start", 10); // integer
		ret = c.encodeRequest(110,map,1);
		assertNotNull(ret);
		ret2 = c.decodeRequest(110,ret.bb,1);
		assertEquals(10,ret2.body.get("start"));
		
		map.clear();
		map.put("start", "100"); // string
		ret = c.encodeRequest(110,map,1);
		assertNotNull(ret);
		ret2 = c.decodeRequest(110,ret.bb,1);
		assertEquals(100,ret2.body.get("start"));		
	}
	
	@Test
	public void testC() throws Exception {
		TlvCodecs codecs = new TlvCodecs("src/test/resources/avenue_conf");
		TlvCodec c = codecs.findTlvCodec(451);
		
		HashMap<String,Object> map = new HashMap<String,Object>();
		ByteBufferWithReturnCode ret;
		MapWithReturnCode ret2;
		
		map.clear();
		
		ArrayList<HashMap<String,Object>> am = new ArrayList<HashMap<String,Object>>();
		HashMap<String,Object> a0 = new HashMap<String,Object>();
		a0.put("id", "100");
		a0.put("num", "13");
		a0.put("name", "360渠道");
		a0.put("remark", "");
		am.add(a0);
		
		map.put("data", am);
		ret = c.encodeResponse(110,map,1);
		assertNotNull(ret);
		ret2 = c.decodeResponse(110,ret.bb,1);
		assertNotNull(ret2);
		assertNotNull(ret2.body.get("data"));
		ArrayList<HashMap<String,Object>> am2 = (ArrayList<HashMap<String,Object>>) ret2.body.get("data");
		assertEquals(1,am2.size());	
		HashMap<String,Object> info2 = am2.get(0);
		assertEquals("100",info2.get("id"));	
		assertEquals("13",info2.get("num"));
		assertEquals("360渠道",info2.get("name"));
		assertEquals("",info2.get("remark"));
	
		map.clear();
		
		am = new ArrayList<HashMap<String,Object>>();
		a0 = new HashMap<String,Object>();
		a0.put("id", "100");
		a0.put("num", "13");
		a0.put("name", "360渠道");
		a0.put("remark", null); // remark传null
		am.add(a0);
		map.put("data", am);
		ret = c.encodeResponse(110,map,1);
		assertNotNull(ret);
		ret2 = c.decodeResponse(110,ret.bb,1);
		assertNotNull(ret2);
		assertNotNull(ret2.body.get("data"));
		am2 = (ArrayList<HashMap<String,Object>>) ret2.body.get("data");
		assertEquals(1,am2.size());	
		info2 = am2.get(0);
		assertEquals("100",info2.get("id"));	
		assertEquals("13",info2.get("num"));
		assertEquals("360渠道",info2.get("name"));
		assertEquals("",info2.get("remark"));
		
		am = new ArrayList<HashMap<String,Object>>();
		a0 = new HashMap<String,Object>();
		a0.put("id", 100); // 改成整数
		a0.put("num", 13); // 改成整数
		a0.put("name", "360渠道");
		a0.put("remark", null); // remark传null
		am.add(a0);
		HashMap<String,Object> a1 = new HashMap<String,Object>();
		a1.put("id", "222"); // 改成整数
		a1.put("num", "555"); // 改成整数
		a1.put("name", "百度");
		a1.put("remark", null); // remark传null
		am.add(a1);		
		map.put("data", am);
		ret = c.encodeResponse(110,map,1);
		assertNotNull(ret);
		ret2 = c.decodeResponse(110,ret.bb,1);
		assertNotNull(ret2);
		assertNotNull(ret2.body.get("data"));
		am2 = (ArrayList<HashMap<String,Object>>) ret2.body.get("data");
		assertEquals(2,am2.size());	
		info2 = am2.get(0);
		assertEquals("100",info2.get("id"));	
		assertEquals("13",info2.get("num"));
		assertEquals("360渠道",info2.get("name"));
		assertEquals("",info2.get("remark"));		
		HashMap<String,Object> info3 = am2.get(1);
		assertEquals("222",info3.get("id"));	
		assertEquals("555",info3.get("num"));
		assertEquals("百度",info3.get("name"));
		assertEquals("",info3.get("remark"));		
	}
	
}

