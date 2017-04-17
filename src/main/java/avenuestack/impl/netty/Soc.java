package avenuestack.impl.netty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.nio.ByteBuffer;

import org.dom4j.Element;
import org.jboss.netty.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import avenuestack.Request;
import avenuestack.Response;
import avenuestack.ErrorCodes;
import avenuestack.impl.avenue.AvenueCodec;
import avenuestack.impl.avenue.AvenueData;
import avenuestack.impl.avenue.ByteBufferWithReturnCode;
import avenuestack.impl.avenue.MapWithReturnCode;
import avenuestack.impl.avenue.TlvCodec;
import avenuestack.impl.avenue.TlvCodec4Xhead;
import avenuestack.impl.avenue.TlvCodecs;
import avenuestack.impl.util.ArrayHelper;
import avenuestack.impl.util.NamedThreadFactory;
import avenuestack.impl.util.QuickTimerEngine;
import avenuestack.impl.util.RequestIdGenerator;

class SocActor implements Actor {

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

    boolean isTrue(String s) {
    	return s != null && s.equals("1") || s.equals("y") || s.equals("t") || s.equals("yes") || s.equals("true");
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
        if( !s.equals("") ) needShakeHands = isTrue(s);

        String shakeHandsTo = "";
        s = cfgNode.attributeValue("shakeHandsTo",""); 
        if( !s.equals("") ) shakeHandsTo = s;

        String shakeHandsPubKey = "";
        s = cfgNode.attributeValue("shakeHandsPubKey",""); 
        if( !s.equals("") ) shakeHandsPubKey = s;

        
        String firstServiceId = serviceIds.split(",")[0];
        threadFactory = new NamedThreadFactory("soc"+firstServiceId);
        pool = new ThreadPoolExecutor(maxThreadNum, maxThreadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(queueSize),threadFactory);
        pool.prestartAllCoreThreads();

        boolean isSps = router.getConfig("isSps","0").equals("1");
        String reportSpsTo = router.getConfig("spsReportTo","55605:1");
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

class SocImpl implements Soc4Netty { // with Logging with Dumpable 

	static Logger log = LoggerFactory.getLogger(SocImpl.class);

	String addrs;
	TlvCodecs codecs;
    //val receiver_f: (Any)=>Unit;
    int retryTimes  = 2;
    int connectTimeout  = 15000;
    int pingInterval= 60000;
    int maxPackageSize = 2000000;
    int connSizePerAddr = 8;
    int timerInterval = 100;
    int reconnectInterval = 1;
    boolean isSps= false;
    String reportSpsTo = "0:0";
    boolean needShakeHands = false;
    String shakeHandsTo = null;
    String shakeHandsPubKey = null;
    
    ThreadPoolExecutor bossExecutor = null;
    ThreadPoolExecutor workerExecutor = null;
    HashedWheelTimer timer  = null;
    QuickTimerEngine qte  = null;
    boolean waitForAllConnected  = false;
    int waitForAllConnectedTimeout  = 60000;
    boolean connectOneByOne  = false;
    boolean reuseAddress = false;
    int startPort = -1;
    Actor actor = null;
	
	ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
	NettyClient nettyClient;
	AtomicInteger generator = new AtomicInteger(1);
	//AvenueCodec converter = new AvenueCodec();
	ConcurrentHashMap<String,String> keyMap = new ConcurrentHashMap<String,String>();
	ConcurrentHashMap<Integer,CacheData> dataMap = new ConcurrentHashMap<Integer,CacheData>();

	ConcurrentHashMap<String,String> shakeHandsKeyMap = new ConcurrentHashMap<String,String>();
    int shakeHandsServiceId = 1;
    int shakeHandsMsgId = 5;

    ConcurrentHashMap<String,String> addrMap = new ConcurrentHashMap<String,String>(); // addr:connidx->connId
    ReentrantLock lock = new ReentrantLock(false);
    HashMap<String,Integer> addrIdxMap = new HashMap<String,Integer>(); // addr->idx
    
    public static ShakeHandsCrypt shakeHandsCrypt;
    
    void dump() {

    	StringBuilder buff = new StringBuilder();

        buff.append("dataMap.size=").append(dataMap.size()).append(",");
        buff.append("keyMap.size=").append(keyMap.size()).append(",");
        buff.append("shakeHandsKeyMap.size=").append(shakeHandsKeyMap.size()).append(",");
        buff.append("addrMap.size=").append(addrMap.size()).append(",");
        buff.append("addrIdxMap.size=").append(addrIdxMap.size()).append(",");

        log.info(buff.toString());

        nettyClient.dump();
    }

    void init() {

        if( shakeHandsTo != null && !shakeHandsTo.equals("")) {
            String[] ss = shakeHandsTo.split(":");
            shakeHandsServiceId = Integer.parseInt( ss[0] );
            shakeHandsMsgId = Integer.parseInt( ss[1] );
        }
        
        nettyClient = new NettyClient(this,addrs);
        nettyClient.connectTimeout = connectTimeout;
        nettyClient.pingInterval = pingInterval;
        nettyClient.maxPackageSize = maxPackageSize;
        nettyClient.connSizePerAddr = connSizePerAddr;
        nettyClient.timerInterval = timerInterval;
        nettyClient.reconnectInterval = reconnectInterval;
        nettyClient.bossExecutor = bossExecutor;
        nettyClient.workerExecutor = workerExecutor;
        nettyClient.timer = timer;
        nettyClient.qte = qte;
        nettyClient.waitForAllConnected = waitForAllConnected;
        nettyClient.waitForAllConnectedTimeout = waitForAllConnectedTimeout;
        nettyClient.connectOneByOne = connectOneByOne;
        nettyClient.reuseAddress = reuseAddress;
        nettyClient.startPort = startPort;
        nettyClient.isSps = isSps;
        		
        nettyClient.init();
        
        nettyClient.start();

        log.info("soc {} started",addrs);
    }

    void close() {
        nettyClient.close();
        log.info("soc {} stopped",addrs);
    }

/*
    def selfcheck() : ArrayBuffer[SelfCheckResult] = {
        val buff = nettyClient.selfcheck()
        buff
    }
*/
    
    int send(AvenueData data,int timeout) {
        try {

        	ByteBuffer buff = AvenueCodec.encode(data);

            boolean ok = nettyClient.send(data.sequence,buff,timeout);
            if( ok ) return 0;
            else return ErrorCodes.NETWORK_ERROR;
        } catch(Exception e) {
                log.error("send exception",e);
                return ErrorCodes.TLV_ERROR;
        }
    }

    boolean isConnId(String addr) {
        int p1 = addr.indexOf(":");
        if( p1 < 0 ) return false;
        int p2 = addr.indexOf(":",p1+1);
        if( p2 < 0 ) return false;
        return true;
    }
    
    String findConnId(String addr) {
        if( connSizePerAddr == 1 ) {
        	String connId = addrMap.get(addr+"-0");
            if( connId != null ) return connId;
            return null;
        }

        lock.lock();
        try {
            Integer idx = addrIdxMap.get(addr);
            if( idx == null ) return null;

            int i = 0 ;
            while( i < connSizePerAddr ) {
                String key = addr+"-"+idx;
                String connId = addrMap.get(key);
                if( connId != null ) {
                    idx += 1;
                    if( idx >= connSizePerAddr ) idx = 0;
                    addrIdxMap.put(addr,idx);
                    return connId;
                }
                idx += 1;
                if( idx >= connSizePerAddr ) idx = 0;
                i += 1;
            }
            addrIdxMap.put(addr,idx);
        } finally {
            lock.unlock();
        }

        return null;
    }
    
    int sendByAddr(AvenueData data,int timeout,String addr) {

    	if( isConnId(addr) ) { // 目前只有shakehands才会这样调用
            return sendByConnId(data,timeout,addr);
        }

        if( needShakeHands ) {
            String connId = findConnId(addr);
            if( connId == null ) return ErrorCodes.NETWORK_ERROR;
            String key = keyMap.get(connId);
            if( key == null ) return ErrorCodes.NETWORK_ERROR;
            return sendByConnId(data,timeout,connId);
        }
    	
        try {

        	ByteBuffer buff = AvenueCodec.encode(data);

            boolean ok = nettyClient.sendByAddr(data.sequence,buff,timeout,addr);
            if( ok ) return 0;
            else return ErrorCodes.NETWORK_ERROR;
        } catch(Exception e) {
                log.error("send exception",e);
                return ErrorCodes.TLV_ERROR;
        }
    }

    int sendByConnId(AvenueData data,int timeout,String connId) {
        try {

        	String key = null;
            if( connId != null && !connId.equals("") )
                key = keyMap.get(connId);

            ByteBuffer buff = AvenueCodec.encode(data,key);

            boolean ok = nettyClient.sendByConnId(data.sequence,buff,timeout,connId);
            if( ok ) return 0; 
            else return ErrorCodes.NETWORK_ERROR;
        } catch(Exception e) {
                log.error("send exception",e);
                return ErrorCodes.TLV_ERROR;
        }
    }

    int sendAck(RawRequest rawReq) {
    	AvenueData data = new AvenueData (
            AvenueCodec.TYPE_RESPONSE,
            rawReq.data.serviceId,
            rawReq.data.msgId,
            rawReq.data.sequence,
            0,
            rawReq.data.encoding,
            AvenueCodec.ACK_CODE,
            EMPTY_BUFFER,
            EMPTY_BUFFER );

        return sendResponse(data,rawReq.connId);
    }

    int sendErrorCode(RawRequest rawReq,int code) {
    	AvenueData data = new AvenueData (
            AvenueCodec.TYPE_RESPONSE,
            rawReq.data.serviceId,
            rawReq.data.msgId,
            rawReq.data.sequence,
            0,
            rawReq.data.encoding,
            code,
            EMPTY_BUFFER,
            EMPTY_BUFFER );

    	return sendResponse(data,rawReq.connId);
    }

    int sendResponse(AvenueData data,String connId) {

        if( connId == null || connId.equals("") )
            return ErrorCodes.NETWORK_ERROR;

        String key = keyMap.get(connId);

        try {
            ByteBuffer buff = AvenueCodec.encode(data,key);
            boolean ok = nettyClient.sendResponse(data.sequence,buff,connId);
            if( ok ) return 0;
            else return ErrorCodes.NETWORK_ERROR;
        } catch(Exception e) {
                return ErrorCodes.TLV_ERROR;
        }
    }


    public void connected(String connId,String addr,int connidx) {
        if( !needShakeHands ) return;
        nettyClient.addChannelToMap(connId);
        addrMap.put(addr+"-"+connidx,connId);
        lock.lock();
        try {
            if( addrIdxMap.get(addr) == null ) {
               addrIdxMap.put(addr,0) ;
            }
        } finally {
            lock.unlock();
        }
        shakeHands(connId);
    }
    public  void disconnected(String connId,String addr,int connidx) {
        if( !needShakeHands ) return;
        addrMap.remove(addr+"-"+connidx);
    }

    void shakeHands(String connId) {
    
        if( shakeHandsCrypt == null ) return ;

        String requestId = "SOC"+RequestIdGenerator.nextId(); // SOC is a special prefix
        String aesKey = java.util.UUID.randomUUID().toString().replaceAll("-", "").substring(0, 16);
        shakeHandsKeyMap.put(requestId,aesKey);
        HashMap<String,Object> body = new HashMap<String,Object>();
        String aesKeyPack = shakeHandsCrypt.shakehands_enc_f(shakeHandsPubKey,aesKey);
        body.put("clientKey",aesKeyPack);

        Request req = new Request(
            requestId,
            "0:0",
            0,
            1,
            shakeHandsServiceId,
            shakeHandsMsgId,
            new HashMap<String,Object>(),
            body,
            actor
        );
        req.setToAddr( connId );
        send(req,30000);
    }

    void afterShakeHands(Request req,Response res) {
        String connId = req.getToAddr();
        boolean ok = false;
        String aesKey = shakeHandsKeyMap.remove(req.getRequestId());
        if( res.getCode() == 0 ) {
        	byte[] serverKeyBytes = (byte[])res.getBody().get("serverKey");
            String serverKey = shakeHandsCrypt.shakehands_dec_f(aesKey,serverKeyBytes);
            if( serverKey != null ) {
                keyMap.put( connId, serverKey );
                ok = true;
                log.info("shakehands ok, soc.key="+serverKey+",connId="+connId);
                return;
            }
            log.error("shakehands error, decrypt buff is null, connId="+connId);

      } else {
          log.error("shakehands error, res="+res.toString());
      }
      if(!ok) {
         nettyClient.closeChannelFromOutside(connId);
      }
    }
    
    public Soc4NettySequenceInfo receive(ByteBuffer res, String connId) {

    	AvenueData data;

    	String key = null;
        if( connId != null && !connId.equals("") )
            key = keyMap.get(connId);

        try {
            data = AvenueCodec.decode(res,key);
        } catch(Exception e) {
                log.error("decode exception");
                Soc4NettySequenceInfo ret = new Soc4NettySequenceInfo(false,0);
                return ret;
        }

        switch(data.flag) {

            case AvenueCodec.TYPE_REQUEST: {

	                // append remote addr to xhead, the last addr is always remote addr
	                try {
	                    data.xhead = TlvCodec4Xhead.appendGsInfo(data.xhead,parseRemoteAddr(connId));
	                } catch(Exception e) {
	                }
	
	                try {
	                    receive(new SosRequest(data,connId));
	                } catch(Exception e) {
	                        log.error("receive exception res={}",data,e);
	                }
	
	                Soc4NettySequenceInfo ret = new Soc4NettySequenceInfo(false,0);
	                return ret;
            	}
            
            case AvenueCodec.TYPE_RESPONSE: {

	                if( isPong(data.serviceId, data.msgId)  ) {
	                	Soc4NettySequenceInfo ret = new Soc4NettySequenceInfo(false,0);
	                    return ret;
	                }
	
	                if( isAck(data.code)  ) {
	
	                    try {
	                        receive(new SocSendAck(data,connId));
	                    } catch(Exception e) {
	                        log.error("receive exception res={}",data,e);
	                    }
	
	                    Soc4NettySequenceInfo ret = new Soc4NettySequenceInfo(false,0);
	                    return ret;
	                }
	
	                try {
	                    receive(new SocSendResponse(data,connId));
	                } catch(Exception e) {
                        log.error("receive exception res={}",data,e);
	                }
    	            Soc4NettySequenceInfo ret = new Soc4NettySequenceInfo(true, data.sequence);
    	            return ret;
	            }

            default:
                log.error("unknown type");

        }
        Soc4NettySequenceInfo ret = new Soc4NettySequenceInfo(true, data.sequence);
        return ret;
    }

    public void networkError(int sequence,String connId) {
        try {
            receive(new SocSendNetworkError(sequence,connId));
        } catch(Exception e) {
            log.error("networkError callback exception");
        }
    }

    public void timeoutError(int sequence,String connId) {

        try {
            receive(new SocSendTimeout(sequence,connId));
        } catch(Exception e) {
                log.error("timeoutError callback exception");
        }

    }

    public ByteBuffer generatePing() {

        int seq = generateSequence();

        AvenueData res = new AvenueData (
            AvenueCodec.TYPE_REQUEST,
            0,
            0,
            seq,
            0,
            0,
            0,
            EMPTY_BUFFER,
            EMPTY_BUFFER );

        ByteBuffer bb = AvenueCodec.encode(res);
        return bb;
    }

    public ByteBuffer generateReportSpsId() {

        if( reportSpsTo.equals("0:0") ) return null;

        int seq = generateSequence();

        HashMap<String,Object> xhead = new HashMap<String,Object>();
        xhead.put(AvenueCodec.KEY_SPS_ID,TlvCodec4Xhead.SPS_ID_0);
        String[] reportSpsInfo = reportSpsTo.split(":");
        ByteBuffer xheadbuff = TlvCodec4Xhead.encode(Integer.parseInt(reportSpsInfo[0]),xhead);
        AvenueData res = new AvenueData (
            AvenueCodec.TYPE_REQUEST,
            Integer.parseInt(reportSpsInfo[0]),
            Integer.parseInt(reportSpsInfo[1]),
            seq,
            0,
            0,
            0,
            xheadbuff,
            EMPTY_BUFFER );

        ByteBuffer bb = AvenueCodec.encode(res);
        return bb;
    }

    boolean isPong(int serviceId,int msgId) { return serviceId == 0 && msgId == 0; }
    boolean isAck(int code) { return code == AvenueCodec.ACK_CODE; }

    int generateSequence() {
        return generator.getAndIncrement();
    }

    public void send(RawRequest rawReq,int timeout) { send(rawReq,timeout,0); }
    public void send(Request req,int timeout) { send(req,timeout,0); }

    void send(RawRequest rawReq,int timeout,int sendTimes) {

    	AvenueData req = rawReq.data;

        int sequence = generateSequence();
        AvenueData data = new AvenueData(
            AvenueCodec.TYPE_REQUEST,
            req.serviceId,
            req.msgId,
            sequence,
            req.mustReach,
            req.encoding,
            req.code,
            req.xhead, req.body );
        dataMap.put(sequence,new CacheData(rawReq,timeout,sendTimes));

        int ret = send(data,timeout);

        if(ret != 0) {
            dataMap.remove(sequence);
            RawResponse rawRes = createErrorResponse(ret,rawReq);
            actor.receive(new RawRequestResponseInfo(rawReq,rawRes) );
        }
    }

    void send(Request req,int timeout,int sendTimes) {

    	TlvCodec tlvCodec = codecs.findTlvCodec(req.getServiceId());
        if( tlvCodec == null ) {
        	Response res = createErrorResponse(ErrorCodes.TLV_ERROR,req);
            actor.receive(new RequestResponseInfo(req,res));
            return;
        }

        int sequence = generateSequence();
        req.setSequence(sequence);
        ByteBuffer xhead = TlvCodec4Xhead.encode(req.getServiceId(),req.getXhead());
        ByteBufferWithReturnCode d = tlvCodec.encodeRequest(req.getMsgId(),req.getBody(),req.getEncoding());
        if( d.ec !=  0 ) {
            log.error("encode request error, serviceId="+req.getServiceId()+", msgId="+req.getMsgId());

            Response res = createErrorResponse(d.ec,req);
            actor.receive(new RequestResponseInfo(req,res));
            return;
        }

        AvenueData data = new AvenueData(
            AvenueCodec.TYPE_REQUEST,
            req.getServiceId(),
            req.getMsgId(),
            sequence,
            0,
            req.getEncoding(),
            0,
            xhead, d.bb );
        dataMap.put(sequence,new CacheData(req,timeout,sendTimes));

        int ret = 0;
        if( req.getToAddr() == null )
            ret = send(data,timeout);
        else
            ret = sendByAddr(data,timeout,req.getToAddr());

        if(ret != 0 ) {
            dataMap.remove(sequence);
            Response res = createErrorResponse(ret,req);
            actor.receive(new RequestResponseInfo(req,res));
        }

    }

    RawResponse createErrorResponse(int code,RawRequest rawReq) {
    	AvenueData data = rawReq.data;
        AvenueData res = new AvenueData(
            AvenueCodec.TYPE_RESPONSE,
            data.serviceId,
            data.msgId,
            data.sequence,
            0,
            data.encoding,
            code,
            EMPTY_BUFFER,EMPTY_BUFFER);
        RawResponse rawRes = new RawResponse(res,rawReq);
        return rawRes;
    }

    Response createErrorResponse(int code,Request req){
    	Response res = new Response (code,new HashMap<String,Object>(),req);
        return res;
    }


    void receive(Object v) {

        if( v instanceof SocSendResponse ) {
        	SocSendResponse vv = (SocSendResponse)v;
        	CacheData saved = dataMap.remove(vv.data.sequence);
            if( saved != null ) {
                if( saved.isRaw) {
                	RawRequest rawReq = (RawRequest)saved.data;
	
                	AvenueData res = new AvenueData(
	                    AvenueCodec.TYPE_RESPONSE,
	                    rawReq.data.serviceId,
	                    rawReq.data.msgId,
	                    rawReq.data.sequence,
	                    0,
	                    rawReq.data.encoding,
	                    vv.data.code,
	                    EMPTY_BUFFER,vv.data.body);
	                RawResponse rawRes = new RawResponse(res,rawReq);
	                rawRes.remoteAddr = parseRemoteAddr(vv.connId);
	                actor.receive(new RawRequestResponseInfo(rawReq,rawRes));
                
                } else {
                	Request req = (Request)saved.data;
	
                	TlvCodec tlvCodec = codecs.findTlvCodec(req.getServiceId());
	                if( tlvCodec != null ) {
	
	                	MapWithReturnCode d = tlvCodec.decodeResponse(req.getMsgId(),vv.data.body,vv.data.encoding);
	                    int errorCode = vv.data.code ;
	                    if( errorCode == 0 && d.ec != 0 ) {
	                        log.error("decode response error, serviceId="+req.getServiceId()+", msgId="+req.getMsgId());
		                    errorCode = d.ec;
	                    }
	
	                    Response res = new Response (errorCode,d.body,req);
	                    res.setRemoteAddr( parseRemoteAddr(vv.connId) );
                        if( needShakeHands && req.getServiceId() == shakeHandsServiceId && req.getMsgId() == shakeHandsMsgId ) {
                            afterShakeHands(req,res);
                            return;
                        }	                    
	                    actor.receive(new RequestResponseInfo(req,res));
	                }
                }
            } else {
                log.warn("receive but sequence not found, seq={}",vv.data.sequence);
            }
            return;
        }

        if( v instanceof SocSendAck ) {
        	SocSendAck vv = (SocSendAck)v;
        
        	CacheData saved = dataMap.get(vv.data.sequence); // donot remove
            if( saved != null ) {
                if( saved.isRaw ) {
                	RawRequest rawReq = (RawRequest)saved.data;	
                    actor.receive(new RawRequestAckInfo(rawReq));
                }else {
                	Request req = (Request)saved.data;    
                    actor.receive(new RequestAckInfo(req));
                }

            } else {
                log.warn("receive but sequence not found, seq={}",vv.data.sequence);
            }
            
        	return;
        }

        if( v instanceof SocSendTimeout ) {
        	SocSendTimeout vv = (SocSendTimeout)v;
        	CacheData saved = dataMap.remove(vv.sequence);
            if( saved != null ) {
            	if( saved.isRaw ) {
            		RawRequest rawReq = (RawRequest)saved.data;	
            		RawResponse rawRes = createErrorResponse(ErrorCodes.SERVICE_TIMEOUT,rawReq);
                    rawRes.remoteAddr = parseRemoteAddr(vv.connId);
                    actor.receive(new RawRequestResponseInfo(rawReq,rawRes));
            	} else {
            		Request req = (Request)saved.data;    
            		Response res = createErrorResponse(ErrorCodes.SERVICE_TIMEOUT,req);
                    res.setRemoteAddr( parseRemoteAddr(vv.connId) );
                    if( needShakeHands && req.getServiceId() == shakeHandsServiceId && req.getMsgId() == shakeHandsMsgId ) {
                        afterShakeHands(req,res);
                        return;
                    }	                    
                    actor.receive(new RequestResponseInfo(req,res));
                }
            } else {
                log.error("timeout but sequence not found, seq={}",vv.sequence);
            }

        	return;
        }

        if( v instanceof SocSendNetworkError ) {
        	SocSendNetworkError vv = (SocSendNetworkError)v;
        	
        	CacheData saved = dataMap.remove(vv.sequence);
            if( saved != null ) {

                saved.sendTimes += 1;
                long now = System.currentTimeMillis();
                if( saved.sendTimes >= retryTimes || now + 30 >= saved.sendTime + saved.timeout ) {

                	if( saved.isRaw ) {
                		RawRequest rawReq = (RawRequest)saved.data;	
                		RawResponse rawRes = createErrorResponse(ErrorCodes.NETWORK_ERROR,rawReq);
                        rawRes.remoteAddr = parseRemoteAddr(vv.connId);
                        actor.receive(new RawRequestResponseInfo(rawReq,rawRes));
                	} else {
                		Request req = (Request)saved.data;    
                		Response res = createErrorResponse(ErrorCodes.NETWORK_ERROR,req);
                        res.setRemoteAddr( parseRemoteAddr(vv.connId) );
                        if( needShakeHands && req.getServiceId() == shakeHandsServiceId && req.getMsgId() == shakeHandsMsgId ) {
                            afterShakeHands(req,res);
                            return;
                        }	                    
                        actor.receive(new RequestResponseInfo(req,res));
                    }
                } else {

                        log.warn("resend data, req={},sendTimes={}",saved.data,saved.sendTimes);

                        if( saved.isRaw ) {
                    		RawRequest rawReq = (RawRequest)saved.data;	
                            send(rawReq,saved.timeout,saved.sendTimes);
                        } else {
                       		Request req = (Request)saved.data;    
                            send(req,saved.timeout,saved.sendTimes);
                        }

                }
            } else {
                log.error("network error but sequence not found, seq={}",vv.sequence);
            }
            
        	return;
        }        	

        if( v instanceof SosRequest ) {
        	SosRequest vv = (SosRequest)v;        

            if( actor == null ) {
            	TlvCodec tlvCodec = codecs.findTlvCodec(vv.data.serviceId);
                if( tlvCodec != null ) {

                    String requestId = RequestIdGenerator.nextId();

                    MapWithReturnCode d = tlvCodec.decodeRequest(vv.data.msgId,vv.data.body,vv.data.encoding);
                    if( d.ec != 0 ) {
                        log.error("decode request error, serviceId="+vv.data.serviceId+", msgId="+vv.data.msgId);
                        
                        AvenueData res = new AvenueData(
                                AvenueCodec.TYPE_RESPONSE,
                                vv.data.serviceId,
                                vv.data.msgId,
                                vv.data.sequence,
                                0,
                                vv.data.encoding,
                                d.ec,
                                EMPTY_BUFFER,
                                EMPTY_BUFFER );

                        int ret = sendResponse(res,vv.connId);
                        if(ret != 0 ) {
                            log.error("send response error");
                        }
                            
                        return;
                    }

                    Request req = new Request(requestId,
                        vv.connId,
                        vv.data.sequence,
                        vv.data.encoding,
                        vv.data.serviceId,
                        vv.data.msgId,
                        new HashMap<String,Object>(),
                        d.body,
                        null);
                    actor.receive(req);
                } else {
                    log.warn("serviceId not found, serviceId={}",vv.data.serviceId);
                }
            } else {
                String requestId = RequestIdGenerator.nextId();
                RawRequest rawReq = new RawRequest(requestId,vv.data,vv.connId,actor);
                actor.receive(rawReq);
            }
            return;
        }
    }

    String parseRemoteAddr(String connId) {

        int p = connId.lastIndexOf(":");

        if (p >= 0)
            return connId.substring(0,p);
        else
        	return "0.0.0.0:0";
    }
    
    class SocSendAck {
    	AvenueData data;
    	String connId;
    	SocSendAck(AvenueData data,String connId) {
    		this.data = data;
    		this.connId = connId;
    	}
    }
    class SocSendResponse {
    	AvenueData data;
    	String connId;
    	SocSendResponse(AvenueData data,String connId) {
    		this.data = data;
    		this.connId = connId;
    	}
    }
    class SocSendTimeout {
    	int sequence;
    	String connId;
    	SocSendTimeout(int sequence,String connId) {
    		this.sequence = sequence;
    		this.connId = connId;
    	}
    }
    class SocSendNetworkError{
    	int sequence;
    	String connId;
    	SocSendNetworkError(int sequence,String connId) {
    		this.sequence = sequence;
    		this.connId = connId;
    	}
    }
    class SosRequest {
    	AvenueData data;
    	String connId;
    	SosRequest(AvenueData data,String connId) {
    		this.data = data;
    		this.connId = connId;
    	}
    }

    class CacheData{
    	
    	boolean isRaw;
    	Object data;
    	long sendTime;
    	int timeout;
    	int sendTimes;
    	
    	CacheData(RawRequest rawReq,int timeout,int sendTimes) {
    		isRaw = true;
    		this.data = rawReq;
    		this.sendTime = System.currentTimeMillis();
    		this.timeout = timeout;
    		this.sendTimes = sendTimes;
    	}
		CacheData(Request req,int timeout,int sendTimes) {
    		isRaw = false;
    		this.data = req;
    		this.sendTime = System.currentTimeMillis();
    		this.timeout = timeout;
    		this.sendTimes = sendTimes;
		}
    }

}



