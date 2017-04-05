package com.sdg.avenuestack.impl.netty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;

import org.junit.Test;

import avenuestack.AvenueStack;
import avenuestack.Request;
import avenuestack.RequestHelper;
import avenuestack.RequestReceiver;
import avenuestack.Response;
import avenuestack.ResponseReceiver;
import avenuestack.impl.netty.AvenueStackImpl;

class AvenueHandler implements RequestReceiver,ResponseReceiver {
	
	public AvenueStack avenueStack;
	
	public void receiveRequest(Request req){
		System.out.println("req:"+req.toString());
		
		HashMap<String,Object> body = new HashMap<String,Object>();
		body.put("total", 100);
		body.put("return_message", "test message");
		avenueStack.sendResponse(0, body, req);		
	}
	public void receiveResponse(Request req,Response res){
		System.out.println("req:"+req.toString());
		System.out.println("res:"+res.toString());
	}
}

public class AvenueStackTest {

	public void test1() throws Exception {
		
		AvenueStackImpl a = new AvenueStackImpl();
		a.setConfDir(".");
		a.setProfile("dev");
		a.init();
		AvenueHandler handler = new AvenueHandler();
		handler.avenueStack = a;
		a.setRequestReceiver(handler);
		a.start();
		
		HashMap<String,Object> body = new HashMap<String,Object>();
		body.put("cur_source", 1);
		body.put("cur_userid", "1001");
		body.put("cur_username", "tony");
		body.put("cur_proj_id", 100);
		Request req = RequestHelper.newRequest("configservice.group_list", body);
		a.sendRequest(req, 3000, handler);
		
		
		Thread.sleep(60000);
		
//		Response res = a.sendRequestWithReturn(req, 3000);
//		System.out.println("res 2:"+res.toString());
//		a.closeReadChannel();
//		a.close();
//		
//		Thread.sleep(1000);
	}
	
	public void test2() throws Exception {
		
		AvenueStackImpl a = new AvenueStackImpl();
		a.setConfDir(".");
		a.setProfile("dev");
		a.init();
		AvenueHandler handler = new AvenueHandler();
		handler.avenueStack = a;
		a.setRequestReceiver(handler);
		a.start();
		
		LinkedHashMap<String,Object> body = new LinkedHashMap<String,Object>();
		body.put("cur_source", 1);
		body.put("cur_userid", "1001");
		body.put("cur_username", "tony");
		body.put("cur_proj_id", 100);
		Request req = RequestHelper.newRequest("configservice.group_list", body);
		a.sendRequest(req, 3000, handler);

		Thread.sleep(10000);
		a.close();
	}	
}

