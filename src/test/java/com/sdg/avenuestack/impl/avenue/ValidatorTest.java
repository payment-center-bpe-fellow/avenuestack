package com.sdg.avenuestack.impl.avenue;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import avenuestack.impl.avenue.Validator;

public class ValidatorTest {

	@Test
	public void testA() throws Exception {
		Validator v = Validator.getValidator("required","","-1");
		int e = v.validate("");
		assertEquals(-1,e);
		v = Validator.getValidator("email","","-1");
		e = v.validate("abc");
		assertEquals(-1,e);
		v = Validator.getValidator("regex","123[a-z]{3}456","-1");
		e = v.validate("123abc456");
		assertEquals(0,e);
		e = v.validate("123ab456");
		assertEquals(-1,e);
		e = v.validate("123abcd456");
		assertEquals(-1,e);
		
	}

}
