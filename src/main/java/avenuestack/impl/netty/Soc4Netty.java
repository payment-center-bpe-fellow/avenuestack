package avenuestack.impl.netty;

import org.jboss.netty.buffer.ChannelBuffer;

//used by netty
public interface Soc4Netty {
    void connected(String connId,String addr,int connidx); 
    void disconnected(String connId,String addr,int connidx); 
	Soc4NettySequenceInfo receive(ChannelBuffer res,String connId); // (true,sequence) or (false,0)
	void networkError(int sequence,String connId);
	void timeoutError(int sequence,String connId);
	ChannelBuffer generatePing();
	ChannelBuffer generateReportSpsId();
}
