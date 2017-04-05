package avenuestack;

import java.util.HashMap;

/**
 * TCP协议是双工， AvenueStack也是如此，每一个长连接上，可以同时扮演服务端和客户端的角色（尽管一般甚少用到）。
 * 
 * 当被人连接的时候， 一般情况是通过AvenueRequestReceiver接受请求，然后调用sendResponse输出响应；
 * 但是服务端也可以通过相同的长连接主动调用sendRequest(使用toAddr属性)发消息给客户端， 然后通过AvenueResponseReceiver接受消息
 * 
 * 当主动连接别人的时候，  一般情况是调用sendRequest发消息，并通过AvenueResponseReceiver接受响应；
 * 但是客户端也可以通过AvenueRequestReceiver监听服务端发过来的请求， 然后调用sendResponse发消息给服务端
 */
public interface AvenueStack {

	// as a server
	void setRequestReceiver(RequestReceiver areqrcv);
	void sendResponse(int code, HashMap<String, Object> body, Request req);
	
	// as a client
	void sendRequest(Request req,int timeout,ResponseReceiver resrcv);
	Response sendRequestWithReturn(Request req,int timeout);

}

