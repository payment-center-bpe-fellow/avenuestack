package com.sdg.avenuestack.impl.netty;

import java.util.HashMap;

import avenuestack.*;
import avenuestack.impl.netty.AvenueStackImpl4Spring;

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

	public static void main(String[] args) throws Exception {
		
		System.setProperty("spring.profiles.active","dev");
		
		AvenueStackImpl4Spring a = new AvenueStackImpl4Spring();
		a.addAvenueXml("classpath:/avenue_conf/talkcore_55621.xml");
		a.addAvenueXml("classpath:/avenue_conf/grammertest/configservice_451.xml");
		a.init();
		a.start();
		
		System.out.println("avenuestack started");
		

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
		
		System.out.println("avenuestack closed");
	}

}

