package avenuestack.impl.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuickTimerEngine {

	static Logger log = LoggerFactory.getLogger(QuickTimerEngine.class);
	
	static AtomicInteger count = new AtomicInteger(1);

	QuickTimerFunction timeoutFunction;
	int checkInterval;

	HashMap< Integer, Queue<QuickTimer> > queueMap = new HashMap< Integer, Queue<QuickTimer> >();
	ConcurrentLinkedQueue< WaitingData > waitingList = new ConcurrentLinkedQueue< WaitingData >();

    AtomicBoolean shutdown = new AtomicBoolean();
    AtomicBoolean shutdownFinished = new AtomicBoolean();

    ArrayList<QuickTimerFunction> timeoutFunctions = new ArrayList<QuickTimerFunction>();
	
    Thread thread = new Thread( new Runnable() {
        public void run() {
            service();
        }
    } );
    
	public QuickTimerEngine(QuickTimerFunction timeoutFunction)  {
		this(timeoutFunction,100);
	}
	
	public QuickTimerEngine (QuickTimerFunction timeoutFunction,int checkInterval)  {
		this.timeoutFunction = timeoutFunction;
		this.checkInterval = checkInterval;
		init();
	}

	public void dump() {

    	StringBuilder buff = new StringBuilder();

        buff.append("threads=1").append(",");
        buff.append("waitingList.size=").append(waitingList.size()).append(",");

        for( Map.Entry<Integer, Queue<QuickTimer>> entry : queueMap.entrySet() ) {
        	int t = entry.getKey();
        	Queue<QuickTimer> q = entry.getValue();
        	buff.append("timeout("+t+").size=").append(q.size()).append(",");
        }
        
        log.info(buff.toString());

    }

    public void init() {

        timeoutFunctions.add( timeoutFunction );
        thread.setName("QuickTimerEngine-"+QuickTimerEngine.count.getAndIncrement());
        thread.start();
    }

    public int registerAdditionalTimeoutFunction(QuickTimerFunction tf){
        timeoutFunctions.add(tf);
        return timeoutFunctions.size()-1;
    }

    public void close() {

        shutdown.set(true);
        thread.interrupt();
        while(!shutdownFinished.get()) {
        	try {
        		Thread.sleep(15);
        	} catch(Exception e) {
        	}
        }

    }

    public QuickTimer newTimer(int timeout,Object data){
    	return newTimer(timeout,data,0);
    }
    
    public QuickTimer newTimer(int timeout,Object data,int timeoutFunctionId){
        long expireTime = System.currentTimeMillis() + timeout;
        QuickTimer timer = new QuickTimer(expireTime,data,new AtomicBoolean(),timeoutFunctionId);
        WaitingData tp = new WaitingData(timeout,timer);
        waitingList.offer(tp);
        return timer;
    }

    void checkTimeout() {

        while( !waitingList.isEmpty() ) {

        	WaitingData wd = waitingList.poll();
        	int timeout = wd.timeout;
        	QuickTimer timer = wd.timer;
        			
        	Queue<QuickTimer> queue = queueMap.get(timeout);
            if( queue == null ) {
                queue = new LinkedList<QuickTimer>();
                queueMap.put(timeout,queue);
            }
            queue.offer(timer);
        }

        long now = System.currentTimeMillis();

        for( Queue<QuickTimer> queue : queueMap.values() ) {

            boolean finished = false;
            while(!finished && !queue.isEmpty() ) {
                QuickTimer first = queue.peek();

                if( first.cancelled.get() ) {
                    queue.remove();
                } else if( first.expireTime <= now ) {

                    if( first.timeoutFunctionId <= 0 ) {

                        try {
                            timeoutFunction.call(first.data);
                        } catch(Exception e){
                                log.error("timer callback function exception e={}",e.getMessage());
                        }
                        
                    } else {

                        if( first.timeoutFunctionId >= timeoutFunctions.size() ) {
                            log.error("timer timeoutFunctionId not found, timeoutFunctionId={}",first.timeoutFunctionId);
                        } else {
                        	QuickTimerFunction tf = timeoutFunctions.get(first.timeoutFunctionId);
                            try {
                                tf.call(first.data);
                            } catch(Exception e) {
                                    log.error("timer callback function exception e={}",e.getMessage());
                            }
                        }


                    }

                    queue.remove();
                } else {
                    finished = true;
                }
            }

        }

    }

    void service() {

        while(!shutdown.get()) {

            try{
                checkTimeout();
            } catch(Exception e) {
                log.error("checkTimeout exception, e={}",e.getMessage());
            }

            try{
                Thread.sleep(checkInterval);
            } catch(Exception e) {
            }
        }

        shutdownFinished.set(true);
    }
    
    static class WaitingData {
    	
    	int timeout;
    	QuickTimer timer;
    	
    	WaitingData(int timeout,QuickTimer timer) {
    		this.timeout = timeout;
    		this.timer = timer;
    	}
    }

}

