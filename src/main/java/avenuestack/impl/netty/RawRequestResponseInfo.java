package avenuestack.impl.netty;

public class RawRequestResponseInfo{
	
	public RawRequest rawReq;
	public RawResponse rawRes;
	
	public RawRequestResponseInfo(RawRequest rawReq, RawResponse rawRes) {
		this.rawReq = rawReq;
		this.rawRes = rawRes;
	}
}
