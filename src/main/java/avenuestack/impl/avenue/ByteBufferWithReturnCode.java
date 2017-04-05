package avenuestack.impl.avenue;

import java.nio.ByteBuffer;

public class ByteBufferWithReturnCode {
	public ByteBuffer bb;
	public int ec;
	
	public ByteBufferWithReturnCode(ByteBuffer bb,int ec) {
		this.bb = bb;
		this.ec = ec;
	}
}

