package avenuestack.impl.netty;

import avenuestack.Request;

public class RequestWithTimeout {
	public Request req;
	public int timeout;
	public RequestWithTimeout(Request req,int timeout) {
		this.req = req;
		this.timeout = timeout;
	}
}


