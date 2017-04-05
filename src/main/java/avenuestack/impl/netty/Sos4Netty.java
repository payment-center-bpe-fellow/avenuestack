package avenuestack.impl.netty;

import java.nio.ByteBuffer;

public interface Sos4Netty {
    void receive(ByteBuffer bb,String connId);
    void connected(String connId);
    void disconnected(String connId);
}
