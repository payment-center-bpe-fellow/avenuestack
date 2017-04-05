package avenuestack.impl.avenue;

import java.nio.ByteBuffer;

public class AvenueData {

    public int flag;
    public int serviceId;
    public int msgId;
    public int sequence;
    public int mustReach;
    public int encoding;
    public int code;
    public ByteBuffer xhead;
    public ByteBuffer body;
    
    public AvenueData() {}
    
    public AvenueData(int flag,
    		int serviceId,
    		int msgId,
    		int sequence,
    		int mustReach,
    		int encoding,
    		int code,
    		ByteBuffer xhead,
    		ByteBuffer body
    		) {
    	this.flag = flag;
    	this.serviceId = serviceId;
    	this.msgId = msgId;
    	this.sequence = sequence;
    	this.mustReach = mustReach;
    	this.encoding = encoding;
    	this.code = code;
    	this.xhead = xhead;
    	this.body = body;
    }

}
