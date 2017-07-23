package avenuestack.impl.avenue;

import org.jboss.netty.buffer.ChannelBuffer;

public class BufferWithReturnCode {
	public ChannelBuffer bb;
	public int ec;
	
	public BufferWithReturnCode(ChannelBuffer bb,int ec) {
		this.bb = bb;
		this.ec = ec;
	}
}

