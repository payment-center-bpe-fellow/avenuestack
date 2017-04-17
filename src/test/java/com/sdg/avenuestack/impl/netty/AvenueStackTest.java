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
		System.out.println("remote req:"+req.toString());
		
		HashMap<String,Object> body = new HashMap<String,Object>();
		body.put("message", req.getBody().get("message"));
		
		System.out.println("remote res:"+body.toString());
		
		avenueStack.sendResponse(0, body, req);		
	}
	public void receiveResponse(Request req,Response res){
		System.out.println("async req:"+req.toString());
		System.out.println("async res:"+res.toString());
	}
}

public class AvenueStackTest {

	//@Test
	public void startServer() throws Exception {
		
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
		body.put("cur_proj_id", 413);
		Request req = RequestHelper.newRequest("configservice.group_list", body);
		a.sendRequest(req, 3000, handler);
		
		Thread.sleep(60000);

		a.closeReadChannel();
		a.close();
	}
	
	//@Test
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
		body.put("cur_proj_id", 413);
		Request req = RequestHelper.newRequest("configservice.group_list", body);
		a.sendRequest(req, 3000, handler);
		
		
		Request req2 = RequestHelper.newRequest("configservice.group_list", body);
		Response res2 = a.sendRequestWithReturn(req2, 3000);
		System.out.println("sync req=:"+req2.toString());
		System.out.println("sync res=:"+res2.toString());
		
		Thread.sleep(5000);

		a.closeReadChannel();
		a.close();
	}
	
	//@Test
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
		body.put("cur_proj_id", 413);
		Request req = RequestHelper.newRequest("configservice.group_list", body);
		a.sendRequest(req, 3000, handler);

		Thread.sleep(5000);
		a.close();
	}	
}

