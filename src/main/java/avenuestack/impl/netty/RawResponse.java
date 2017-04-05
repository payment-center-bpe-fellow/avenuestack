package avenuestack.impl.netty;

import avenuestack.impl.avenue.AvenueData;

public class RawResponse {

	public AvenueData data;
	public String connId;
	public long receivedTime = System.currentTimeMillis();
	public String remoteAddr = "";
	
	public RawResponse( AvenueData data,String connId) {
		this.data = data;
		this.connId = connId;
	}
	public RawResponse( AvenueData data, RawRequest rawReq) {
        this(data,rawReq.connId);
    }

}
