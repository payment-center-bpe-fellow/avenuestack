package com.sdg.avenuestack.impl.avenue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import java.util.ArrayList;
import java.util.HashMap;

import org.jboss.netty.buffer.ChannelBuffer;
import org.junit.Assert;
import org.junit.Test;

import avenuestack.impl.avenue.AvenueCodec;
import avenuestack.impl.avenue.TlvCodec4Xhead;
import avenuestack.impl.avenue.Xhead;

public class TlvCodec4XheadTest {

	@Test
	public void testA() throws Exception {
		HashMap<String,Object> map = new HashMap<String,Object>();
		ChannelBuffer b = TlvCodec4Xhead.encode(100,map,1);
		assertNotNull(b);
		HashMap<String,Object> m2 = TlvCodec4Xhead.decode(100,b);
		assertNotNull(m2);
	}

	@Test
	public void testB() throws Exception {
		HashMap<String,Object> map = new HashMap<String,Object>();
		map.put(Xhead.KEY_SOC_ID,"1.1.1.1:1111");
		map.put(Xhead.KEY_UNIQUE_ID,"2222");
		map.put(Xhead.KEY_SPS_ID,"3333");
		ArrayList<String> gsInfos = new ArrayList<String>();
		gsInfos.add("1.1.1.1:2191");
		gsInfos.add("1.1.1.2:2192");
		gsInfos.add("1.1.1.3:2193");
		map.put(Xhead.KEY_ADDRS,gsInfos);
		ChannelBuffer b = TlvCodec4Xhead.encode(100,map,1);
		assertNotNull(b);
		HashMap<String,Object> m2 = TlvCodec4Xhead.decode(100,b);
		assertNotNull(m2);
		assertEquals(map.get(Xhead.KEY_SOC_ID),m2.get(Xhead.KEY_SOC_ID));
		assertEquals(map.get(Xhead.KEY_UNIQUE_ID),m2.get(Xhead.KEY_UNIQUE_ID));
		assertEquals(map.get(Xhead.KEY_SPS_ID),m2.get(Xhead.KEY_SPS_ID));
		ArrayList<String> gsInfos2 = (ArrayList<String>)m2.get(Xhead.KEY_ADDRS);
		assertEquals(3,gsInfos2.size());
		assertEquals("1.1.1.1:2191",gsInfos2.get(0));
		assertEquals("1.1.1.2:2192",gsInfos2.get(1));
		assertEquals("1.1.1.3:2193",gsInfos2.get(2));
	}

	@Test
	public void testC() throws Exception {
		HashMap<String,Object> map = new HashMap<String,Object>();
		map.put(Xhead.KEY_SPS_ID,3333);  // integer
		ChannelBuffer b = TlvCodec4Xhead.encode(100,map,1);
		assertNotNull(b);
		HashMap<String,Object> m2 = TlvCodec4Xhead.decode(100,b);
		assertNotNull(m2);
		assertEquals("3333",m2.get(Xhead.KEY_SPS_ID));
	}
	
	@Test
	public void testD() throws Exception {
		HashMap<String,Object> map = new HashMap<String,Object>();
		map.put("appId","1000");
		map.put("areaId","1001");
		ChannelBuffer b = TlvCodec4Xhead.encode(100,map,1);
		assertNotNull(b);
		HashMap<String,Object> m2 = TlvCodec4Xhead.decode(100,b);
		assertNotNull(m2);
		assertEquals(1000,m2.get("appId"));
		assertEquals(1001,m2.get("areaId"));
	}
	
	@Test
	public void testE() throws Exception {
		HashMap<String,Object> map = new HashMap<String,Object>();
		map.put(Xhead.KEY_SOC_ID,"12345");
		ChannelBuffer b = TlvCodec4Xhead.encode(3,map,1);  // 服务号是3，有特殊处理
		assertNotNull(b);
		HashMap<String,Object> m2 = TlvCodec4Xhead.decode(3,b);
		assertNotNull(m2);
		assertEquals("12345",m2.get(Xhead.KEY_SOC_ID));
	}	
	
	@Test
	public void testF() throws Exception {
		HashMap<String,Object> map = new HashMap<String,Object>();
		map.put(Xhead.KEY_SOC_ID,"12345");
		ChannelBuffer b1 = TlvCodec4Xhead.encode(3,map,1);  // 服务号是3，有特殊处理
		assertNotNull(b1);
		assertEquals(32,b1.writerIndex());
		ChannelBuffer b2 = TlvCodec4Xhead.encode(13,map,1);  // 服务号不是3
		Assert.assertNotEquals(b1.writerIndex(),b2.writerIndex());
	}	

	
}
