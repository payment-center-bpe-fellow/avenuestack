package avenuestack.impl.netty;

import org.jboss.netty.buffer.ChannelBuffer;

public interface Sos4Netty {
    void receive(ChannelBuffer bb,String connId);
    void connected(String connId);
    void disconnected(String connId);
}
