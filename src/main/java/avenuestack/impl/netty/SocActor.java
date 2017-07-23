package avenuestack.impl.netty;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import avenuestack.Request;
import avenuestack.impl.util.ArrayHelper;
import avenuestack.impl.util.NamedThreadFactory;
import avenuestack.impl.util.TypeSafe;

public class SocActor implements Actor {

	static Logger log = LoggerFactory.getLogger(SocActor.class);

	AvenueStackImpl router;
	Element cfgNode;	
	
    int timeout = 30000;
    		
    String serviceIds;

    int queueSize = 20000;
    int maxThreadNum = 2;
    ThreadFactory threadFactory;
    ThreadPoolExecutor pool;

    SocImpl socWrapper;

    public SocImpl getSocImpl() {
    	return socWrapper;
    }
    
    SocActor(AvenueStackImpl router,Element cfgNode) {
    	this.router = router;
    	this.cfgNode = cfgNode;
    	init();
    }
    
    void dump() {

        log.info("--- serviceIds="+serviceIds);

        StringBuilder buff = new StringBuilder();

        buff.append("pool.size=").append(pool.getPoolSize()).append(",");
        buff.append("pool.getQueue.size=").append(pool.getQueue().size()).append(",");

        log.info(buff.toString());

        socWrapper.dump();
    }

    void init() {

    	serviceIds = ((Element)cfgNode.selectSingleNode("ServiceId")).getText();
    	
        List<Element> addrElements = cfgNode.selectNodes("ServerAddr");
        ArrayList<String> addrsList = new ArrayList<String>();
		for (Element t : addrElements) {
			addrsList.add(t.getText());
		}
        String addrs = ArrayHelper.mkString(addrsList,",");

        String s = cfgNode.attributeValue("threadNum","");
        if( !s.equals("") ) maxThreadNum = Integer.parseInt(s);

        s = cfgNode.attributeValue("timeout","");
        if(!s.equals("") ) timeout = Integer.parseInt(s);

        int retryTimes = 2;
        s = cfgNode.attributeValue("retryTimes","");
        if( !s.equals("") ) retryTimes = Integer.parseInt(s);

        int connectTimeout = 15000;
        s = cfgNode.attributeValue("connectTimeout","");
        if( !s.equals("") ) connectTimeout = Integer.parseInt(s);

        int pingInterval = 60000;
        s = cfgNode.attributeValue("pingInterval","");
        if( !s.equals("") ) pingInterval = Integer.parseInt(s);

        int maxPackageSize = 2000000;
        s = cfgNode.attributeValue("maxPackageSize","");
        if( !s.equals("") ) maxPackageSize = Integer.parseInt(s);

        int connSizePerAddr = 8;
        s = cfgNode.attributeValue("connSizePerAddr","");
        if( !s.equals("") ) connSizePerAddr = Integer.parseInt(s);

        int timerInterval  = 100;
        s = cfgNode.attributeValue("timerInterval","");
        if( !s.equals("") ) timerInterval = Integer.parseInt(s);

        int reconnectInterval  = 1;
        s = cfgNode.attributeValue("reconnectInterval","");
        if( !s.equals("") ) reconnectInterval = Integer.parseInt(s);

        boolean needShakeHands = false;
        s = cfgNode.attributeValue("needShakeHands","");
        if( !s.equals("") ) needShakeHands = TypeSafe.isTrue(s);

        String shakeHandsTo = "";
        s = cfgNode.attributeValue("shakeHandsTo",""); 
        if( !s.equals("") ) shakeHandsTo = s;

        String shakeHandsPubKey = "";
        s = cfgNode.attributeValue("shakeHandsPubKey",""); 
        if( !s.equals("") ) shakeHandsPubKey = s;

        int pingVersion  = 1;
        s = cfgNode.attributeValue("pingVersion","");
        if( !s.equals("") ) pingVersion = Integer.parseInt(s);
        
        String firstServiceId = serviceIds.split(",")[0];
        threadFactory = new NamedThreadFactory("soc"+firstServiceId);
        pool = new ThreadPoolExecutor(maxThreadNum, maxThreadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(queueSize),threadFactory);
        pool.prestartAllCoreThreads();

        boolean isSps = router.getParameter("isSps","0").equals("1");
        String reportSpsTo = router.getParameter("spsReportTo","55605:1");
        String reportSpsServiceId = reportSpsTo.split(":")[0];
        String[] ss = serviceIds.split(",");
        boolean found = false;
        for( String s1 : ss ) {
        	if( s1.equals(reportSpsServiceId) ) found = true;
        }
        if(!found) reportSpsTo="0:0";
        

        socWrapper = new SocImpl();
        socWrapper.addrs = addrs;
        socWrapper.codecs = router.tlvCodecs;
        //socWrapper.receive = this.receive;
        socWrapper.retryTimes = retryTimes;
        socWrapper.connectTimeout = connectTimeout;
        socWrapper.pingInterval = pingInterval;
        socWrapper.maxPackageSize = maxPackageSize;
        socWrapper.connSizePerAddr = connSizePerAddr;
        socWrapper.timerInterval = timerInterval;
        socWrapper.reconnectInterval = reconnectInterval;
        socWrapper.isSps = isSps;
        socWrapper.reportSpsTo = reportSpsTo;
        socWrapper.needShakeHands = needShakeHands;
        socWrapper.shakeHandsTo = shakeHandsTo;
        socWrapper.shakeHandsPubKey = shakeHandsPubKey;
        socWrapper.pingVersion = pingVersion;
        socWrapper.actor = this;
        
        socWrapper.init();

        log.info("SocActor started {}",serviceIds);
    }

    void close() {

        long t1 = System.currentTimeMillis();

        pool.shutdown();

        try {
        	pool.awaitTermination(5,TimeUnit.SECONDS);
        } catch(Exception e) {
        }

        long t2 = System.currentTimeMillis();
        if( t2 - t1 > 100 )
            log.warn("SocActor long time to shutdown pool, ts={}",t2-t1);


        socWrapper.close();
        log.info("SocActor stopped {}",serviceIds);
    }

    public void receive(final Object v) {

        try {
            pool.execute( new Runnable() {
                public void run() {
                    try {
                        onReceive(v);
                    } catch(Exception e) {
                            log.error("soc exception v={}",v,e);
                    }
                }
            });
        } catch(RejectedExecutionException e) {
            // ignore the message
            log.error("soc queue is full, serviceIds={}",serviceIds);
        }
    }

    void onReceive(Object v) {

        if( v instanceof RequestWithTimeout ) {
        	RequestWithTimeout reqtimeout = (RequestWithTimeout)v;
        	socWrapper.send(reqtimeout.req,reqtimeout.timeout);
        	return;
        }
    	
        if( v instanceof Request ) {
        	Request req = (Request)v;
        	socWrapper.send(req,timeout);
        	return;
        }

        if( v instanceof RequestResponseInfo ) {
        	RequestResponseInfo reqResInfo = (RequestResponseInfo)v;
        	router.receiveResponse(reqResInfo);
        	return;
        }

        if( v instanceof RequestAckInfo ) {
        	RequestAckInfo reqAckInfo = (RequestAckInfo)v;
        	router.receiveAck(reqAckInfo);
        	return;
        }

        if( v instanceof RawRequest ) {
        	RawRequest rawReq = (RawRequest)v;
        	if( rawReq.sender == this )
                router.receiveRequest(rawReq);
            else
                socWrapper.send(rawReq,timeout);
        	return;
        }

        if( v instanceof RawResponse ) {
        	RawResponse rawRes = (RawResponse)v;
        	socWrapper.sendResponse(rawRes.data,rawRes.connId);
        	return;
        }
        
        if( v instanceof RawRequestResponseInfo ) {
        	RawRequestResponseInfo reqResInfo = (RawRequestResponseInfo)v;
        	if( reqResInfo.rawReq.sender == this ){
                socWrapper.sendResponse(reqResInfo.rawRes.data,reqResInfo.rawRes.connId);
            }else{
                router.receiveResponse(reqResInfo);
            }
        	return;
        }

        if( v instanceof RawRequestAckInfo ) {
        	RawRequestAckInfo reqAckInfo = (RawRequestAckInfo)v;
        	if( reqAckInfo.rawReq.sender == this )
                socWrapper.sendAck(reqAckInfo.rawReq);
            else
                router.receiveAck(reqAckInfo);
        	return;
        }
    
        log.error("unknown msg");
    }
    
    /*
    def selfcheck() : ArrayBuffer[SelfCheckResult] = {
        val buff = socWrapper.selfcheck()
        buff
    }
    */
}


