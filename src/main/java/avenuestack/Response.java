package avenuestack;

import java.util.HashMap;

public class Response {

	private String requestId; // 请求ID
	
	private long receivedTime; // 消息收到的时间

	private int code; // 错误码
	
	private HashMap<String,Object> body; // 业务数据

	private String remoteAddr = "";
			
	public Response(int code,HashMap<String,Object> body,Request req) {
		this.code = code;
		this.body = body;
		this.requestId = req.getRequestId();
		receivedTime = System.currentTimeMillis();
	}

	
	public String toString() {
		StringBuilder buff = new StringBuilder();
		buff.append("requestId="+requestId);
		buff.append(",receivedTime="+receivedTime);
		buff.append(",code="+code);
		buff.append(",body="+body);
		buff.append(",remoteAddr="+remoteAddr);
		return buff.toString();
	}
	
	public WrappedMap body() {
		return new WrappedMap(body);
	}
	
	public long getReceivedTime() {
		return receivedTime;
	}

	public void setReceivedTime(long receivedTime) {
		this.receivedTime = receivedTime;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public HashMap<String, Object> getBody() {
		return body;
	}

	public void setBody(HashMap<String, Object> body) {
		this.body = body;
	}

	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public String getRemoteAddr() {
		return remoteAddr;
	}

	public void setRemoteAddr(String remoteAddr) {
		this.remoteAddr = remoteAddr;
	}
}

