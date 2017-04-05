package avenuestack.impl.netty;

import avenuestack.Request;
import avenuestack.Response;

public class RequestResponseInfo {
	
	public Request req; 
	public Response res; 
	
	public RequestResponseInfo(Request req, Response res) {
		this.req = req;
		this.res = res;
	}
}



