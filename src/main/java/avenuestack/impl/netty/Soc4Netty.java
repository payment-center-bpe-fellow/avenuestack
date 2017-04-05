package avenuestack.impl.netty;

import java.nio.ByteBuffer;

//used by netty
public interface Soc4Netty {
	Soc4NettySequenceInfo receive(ByteBuffer res,String connId); // (true,sequence) or (false,0)
	void networkError(int sequence,String connId);
	void timeoutError(int sequence,String connId);
	ByteBuffer generatePing();
	ByteBuffer generateReportSpsId();
}