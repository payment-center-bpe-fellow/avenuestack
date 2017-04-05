package avenuestack.impl.netty;

import avenuestack.impl.avenue.AvenueData;

public class RawRequest {

	public String requestId;
	public AvenueData data;
	public String connId;
	public Actor sender;
	public long receivedTime = System.currentTimeMillis();
	public long persistId = 0L; // for "must reach" message
	
	public RawRequest( String requestId, AvenueData data,String connId, Actor sender) {
		this.requestId = requestId;
		this.data = data;
		this.connId = connId;
		this.sender = sender;
	}

	public String remoteIp(){
        if( connId == null || connId.equals("") ) return null;
        int p = connId.indexOf(":");
	    if( p == -1 ) return null;
	    return connId.substring(0,p);
    }

}


