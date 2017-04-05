package avenuestack.impl.util;

import java.util.concurrent.atomic.AtomicBoolean;

public class QuickTimer {

	long expireTime;
	Object data;
	AtomicBoolean cancelled;
	int timeoutFunctionId = 0;
	
	public QuickTimer (long expireTime,Object data,AtomicBoolean cancelled) {
		this(expireTime,data,cancelled,0);
	}
	public QuickTimer (long expireTime,Object data,AtomicBoolean cancelled, int timeoutFunctionId) {
		this.expireTime = expireTime;
		this.data = data;
		this.cancelled = cancelled;
		this.timeoutFunctionId = timeoutFunctionId;
	}
	
    public void cancel() {
        cancelled.set(true);
    }
}


