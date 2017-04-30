package avenuestack.impl.netty;

import org.jboss.netty.handler.codec.http.HttpResponse;

public interface HttpClient4Netty {
	 void receive(int sequence,HttpResponse httpRes);
	 void networkError(int sequence);
	 void timeoutError(int sequence);
}
