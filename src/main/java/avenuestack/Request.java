package avenuestack;

import java.util.HashMap;

import avenuestack.impl.netty.Actor;

public class Request {

	private String requestId; // 请求ID
	private String connId; // 连接ID
	private int sequence; // SEQUENCE
	private int encoding = 1; // utf-8

	private int serviceId; // 服务号
	private int msgId; // 消息号
	
	private HashMap<String,Object> xhead; // 扩展包头，存调用链地址，唯一ID等
	private HashMap<String,Object> body; // 业务数据
	
	private String toAddr; // 目标地址，只用在从服务端反向调用客户端的时候
	private long receivedTime; // 消息收到的时间
	private Object sender;

	private int version = 1;
	
	public Request() {
	}
	
	public Request (
            String requestId,
            String connId,
            int sequence,
            int encoding,
            int serviceId,
            int msgId,
            HashMap<String,Object> xhead,
            HashMap<String,Object>  body,
            Actor sender
        ) {
		
		this.requestId = requestId;
		this.connId = connId;
		this.sequence = sequence;
		this.encoding = encoding;
		this.serviceId = serviceId;
		this.msgId = msgId;
		this.xhead = xhead;
		this.body = body;
		this.sender = sender;
		this.receivedTime = System.currentTimeMillis();
	}
	
	public String toString() {
		StringBuilder buff = new StringBuilder();
		buff.append("requestId="+requestId);
		buff.append(",connId="+connId);
		buff.append(",sequence="+sequence);
		buff.append(",encoding="+encoding);
		buff.append(",serviceId="+serviceId);
		buff.append(",msgId="+msgId);
		buff.append(",xhead="+xhead);
		buff.append(",body="+body);
		buff.append(",toAddr="+toAddr);
		buff.append(",receivedTime="+receivedTime);
		return buff.toString();
	}
	
	public WrappedMap xhead() {
		return new WrappedMap(xhead);
	}

	public WrappedMap body() {
		return new WrappedMap(body);
	}
	
	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public String getConnId() {
		return connId;
	}

	public void setConnId(String connId) {
		this.connId = connId;
	}

	public int getSequence() {
		return sequence;
	}

	public void setSequence(int sequence) {
		this.sequence = sequence;
	}

	public long getReceivedTime() {
		return receivedTime;
	}

	public void setReceivedTime(long receivedTime) {
		this.receivedTime = receivedTime;
	}

	public int getServiceId() {
		return serviceId;
	}

	public void setServiceId(int serviceId) {
		this.serviceId = serviceId;
	}

	public int getMsgId() {
		return msgId;
	}

	public void setMsgId(int msgId) {
		this.msgId = msgId;
	}

	public HashMap<String, Object> getXhead() {
		return xhead;
	}

	public void setXhead(HashMap<String, Object> xhead) {
		this.xhead = xhead;
	}

	public HashMap<String, Object> getBody() {
		return body;
	}

	public void setBody(HashMap<String, Object> body) {
		this.body = body;
	}

	public String getToAddr() {
		return toAddr;
	}

	public void setToAddr(String toAddr) {
		this.toAddr = toAddr;
	}
	public int getEncoding() {
		return encoding;
	}

	public void setEncoding(int encoding) {
		this.encoding = encoding;
	}

	public Object getSender() {
		return sender;
	}

	public void setSender(Object sender) {
		this.sender = sender;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	
}

