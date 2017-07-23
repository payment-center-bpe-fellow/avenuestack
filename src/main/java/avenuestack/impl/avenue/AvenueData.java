package avenuestack.impl.avenue;

import org.jboss.netty.buffer.ChannelBuffer;

public class AvenueData {

	public int flag;
	public int version;
	public int serviceId;
	public int msgId;
	public int sequence;
	public int mustReach;
	public int encoding;
	public int code;
	public ChannelBuffer xhead;
	public ChannelBuffer body;

	public AvenueData() {
	}

	public AvenueData(int flag, int version, int serviceId, int msgId, int sequence, int mustReach, int encoding,
			int code, ChannelBuffer xhead, ChannelBuffer body) {
		this.flag = flag;
		this.version = version;
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
