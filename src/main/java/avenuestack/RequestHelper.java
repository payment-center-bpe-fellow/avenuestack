package avenuestack;

import java.util.HashMap;

public class RequestHelper {

	// 框架启动时会初始化mappings
	// 格式： servicename.msgname -> serviceId.msgId
	// 示例：testservice.testmsg -> 111.222
	public static HashMap<String,String> mappings = new HashMap<String,String>(); 
	
	public static Request newRequest(String serviceNameMsgName,HashMap<String,Object> body) {
		return newRequest(serviceNameMsgName,null,body);
	}
	public static Request newRequest(int serviceId,int msgId,HashMap<String,Object> body) {
		return newRequest(serviceId,msgId,null,body);
	}

	public static Request newRequest(String serviceNameMsgName,HashMap<String,Object> xhead,HashMap<String,Object> body) {
		return newRequest(serviceNameMsgName,xhead,body,null);
	}
	public static Request newRequest(int serviceId,int msgId,HashMap<String,Object> xhead,HashMap<String,Object> body) {
		return newRequest(serviceId,msgId,xhead,body,null);
	}

	public static Request newRequest(String serviceNameMsgName,HashMap<String,Object> xhead,HashMap<String,Object> body,String toAddr) {
		String ids = mappings.get(serviceNameMsgName.toLowerCase());
		int serviceId = 0;
		int msgId = 0;
		if( ids != null ) {
			String[] ss = ids.split("\\.");
			serviceId = Integer.parseInt(ss[0]);
			msgId = Integer.parseInt(ss[1]);
		}
		return newRequest(serviceId,msgId,xhead,body,toAddr);
	}
	
	public static Request newRequest(int serviceId,int msgId,HashMap<String,Object> xhead,HashMap<String,Object> body,String toAddr) {
		Request req = new Request();
		req.setReceivedTime(System.currentTimeMillis());
		req.setServiceId(serviceId);
		req.setMsgId(msgId);
		req.setXhead(xhead);
		req.setBody(body);
		req.setToAddr(toAddr);
		return req;
	}

}

