package com.sdg.avenuestack.impl.avenue;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import avenuestack.impl.avenue.Encoder;

public class EncoderTest {

	@Test
	public void testA() throws Exception {
		Encoder en = Encoder.getEncoder("escapeencoder","");
		String e = (String)en.encode("abc");
		assertEquals("abc",e);
	}

	@Test
	public void testB() throws Exception {
		Encoder en = Encoder.getEncoder("htmlencoder","");
		
		// "&,&amp;|<,&lt;|>,&gt;|\",&quot;|',&#x27;|/,&#x2f;"
		
		String e = (String)en.encode("&");
		assertEquals("&amp;",e);
		e = (String)en.encode("<");
		assertEquals("&lt;",e);
		e = (String)en.encode(">");
		assertEquals("&gt;",e);
		e = (String)en.encode("\"");
		assertEquals("&quot;",e);
		e = (String)en.encode("'");
		assertEquals("&#x27;",e);
		e = (String)en.encode("/");
		assertEquals("&#x2f;",e);
	
		en = Encoder.getEncoder("htmlfilter","");
		e = (String)en.encode("1&<>\"'/2");
		assertEquals("12",e);
	}
	

	@Test
	public void testC() throws Exception {
		Encoder en = Encoder.getEncoder("normalencoder","1,a|2,b|3,c");

		String e = (String)en.encode("123");
		assertEquals("abc",e);
		
	}	
	
	@Test
	public void testD() throws Exception {
		Encoder en = Encoder.getEncoder("htmlencoder","");
		
		// "&,&amp;|<,&lt;|>,&gt;|\",&quot;|',&#x27;|/,&#x2f;"
		
		String e = (String)en.encode("&");
		assertEquals("&amp;",e);
		String e2 = (String)en.encode(e);
		assertEquals(e,e2);
		
		e = (String)en.encode("&abcde");
		assertEquals("&amp;abcde",e);
		e2 = (String)en.encode(e);
		assertEquals(e,e2);		
	}
	
	@Test
	public void testE() throws Exception {
		Encoder en = Encoder.getEncoder("attackfilter","");
		
		String e = (String)en.encode("script,exec,select,update,delete,insert,create");
		assertEquals(",,,,,,",e);
		e = (String)en.encode("scriptexecselectupdatedeleteinsertcreate");
		assertEquals("",e);
		
		e = (String)en.encode("alter,drop,truncate,&,<,>,\",',/,\\");
		assertEquals(",,,,,,,,,",e);
	}
	
	@Test
	public void testF() throws Exception {
		Encoder en = Encoder.getEncoder("maskencoder","phone");
		
		String e = (String)en.encode("13519080111");
		assertEquals("135****0111",e);
	}	
	
	@Test
	public void testH() throws Exception {
		Encoder en = Encoder.getEncoder("maskencoder","common:1:1");
		
		String e = (String)en.encode("X351908011Y");
		assertEquals("X****Y",e);
	}	
	
	
}
