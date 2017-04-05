package com.sdg.avenuestack.impl.avenue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;

import org.junit.Test;

import avenuestack.impl.avenue.AvenueCodec;
import avenuestack.impl.avenue.AvenueData;

public class AvenueCodecTest {

	@Test
	public void testA() throws Exception {
		AvenueData data = new AvenueData();
		data.flag = AvenueCodec.TYPE_REQUEST;
		data.xhead = ByteBuffer.allocate(0);
		data.body = ByteBuffer.allocate(0);
		
		ByteBuffer b = AvenueCodec.encode(data);
		assertNotNull(b);
		AvenueData data2 = AvenueCodec.decode(b);
		assertNotNull(data2);
		assertEquals(data.flag,data2.flag);
		assertEquals(data.serviceId,data2.serviceId);
		assertEquals(data.msgId,data2.msgId);
		assertEquals(data.sequence,data2.sequence);
		assertEquals(data.mustReach,data2.mustReach);
		assertEquals(data.encoding,data2.encoding);
		assertEquals(data.code,data2.code);
		assertEquals(data.xhead,data2.xhead);
		assertEquals(data.body,data2.body);
	}

	@Test
	public void testB() throws Exception {
		AvenueData data = new AvenueData();
		data.flag = AvenueCodec.TYPE_REQUEST;
		
		data.serviceId = 100;
		data.msgId = 101;
		data.sequence = 102;
		data.mustReach = 1;
		data.encoding = 1;
		data.code = -1029;
		
		data.xhead = ByteBuffer.allocate(0);
		data.body = ByteBuffer.allocate(0);
		
		ByteBuffer b = AvenueCodec.encode(data);
		assertNotNull(b);
		AvenueData data2 = AvenueCodec.decode(b);
		assertNotNull(data2);
		assertEquals(data.flag,data2.flag);
		assertEquals(data.serviceId,data2.serviceId);
		assertEquals(data.msgId,data2.msgId);
		assertEquals(data.sequence,data2.sequence);
		assertEquals(data.mustReach,data2.mustReach);
		assertEquals(data.encoding,data2.encoding);
		assertEquals(0,data2.code);
		assertEquals(data.xhead,data2.xhead);
		assertEquals(data.body,data2.body);
	}
	
	@Test
	public void testC() throws Exception {
		AvenueData data = new AvenueData();
		data.flag = AvenueCodec.TYPE_RESPONSE;
		
		data.serviceId = 100;
		data.msgId = 101;
		data.sequence = 102;
		data.mustReach = 1;
		data.encoding = 1;
		data.code = -1029;
		
		data.xhead = ByteBuffer.allocate(0);
		data.body = ByteBuffer.allocate(0);
		
		ByteBuffer b = AvenueCodec.encode(data);
		assertNotNull(b);
		AvenueData data2 = AvenueCodec.decode(b);
		assertNotNull(data2);
		assertEquals(data.flag,data2.flag);
		assertEquals(data.serviceId,data2.serviceId);
		assertEquals(data.msgId,data2.msgId);
		assertEquals(data.sequence,data2.sequence);
		assertEquals(data.mustReach,data2.mustReach);
		assertEquals(data.encoding,data2.encoding);
		assertEquals(data.code,data2.code);
		assertEquals(data.xhead,data2.xhead);
		assertEquals(data.body,data2.body);
	}
	
}
