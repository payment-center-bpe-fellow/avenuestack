package avenuestack.impl.netty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.ByteBuffer;

import org.dom4j.Element;
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
import avenuestack.impl.util.QuickTimer;
import avenuestack.impl.util.QuickTimerEngine;
import avenuestack.impl.util.RequestIdGenerator;
import avenuestack.impl.util.QuickTimerFunction;

import java.util.concurrent.locks.ReentrantLock;

public class Sos implements Sos4Netty,Actor { 

	static Logger log = LoggerFactory.getLogger(Sos.class);
	
	AvenueStackImpl router;
	int port;
	
	ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
	NettyServer nettyServer;

    // AvenueCodec converter = new AvenueCodec();

    int threadNum = 2;
    int queueSize = 20000;
    ThreadFactory threadFactory;
    ThreadPoolExecutor pool;

    int timeout = 30000;
    int idleTimeoutMillis = 180000;
    int maxPackageSize = 2000000;
    int maxConns = 500000;
    String host = "*";
    int timerInterval = 100;

    ConcurrentHashMap<Integer,CacheData> dataMap = new ConcurrentHashMap<Integer,CacheData>();
    AtomicInteger generator = new AtomicInteger(1);
    TlvCodecs codecs;
    QuickTimerEngine qte;

    String reverseServiceIds = "0";
    boolean hasReverseServiceIds = false;
    String spsDisconnectNotifyTo = "55605:111";
    HashSet<String> reverseIps = null;
    boolean pushToIpPort = false;
    HashMap<String,String> connsAddrMap = new HashMap<String,String>();
    boolean pushToIp = false;
    HashMap<String,ArrayList<String>> connsIpMap = new HashMap<String,ArrayList<String>>();
    HashMap<String,Integer> connsIpIdxMap = new HashMap<String,Integer>();
    boolean pushToAny = false;
    ArrayList<String> allConns = new ArrayList<String>();
    int allConnsIdx = 0;
    ReentrantLock connsLock = new ReentrantLock(false);

    boolean isSps = false;
    boolean isEncrypted = false;
    String shakeHandsServiceIdMsgId = "1:5";
    ConcurrentHashMap<String,String> aesKeyMap = new ConcurrentHashMap<String,String>();
    
	public Sos(AvenueStackImpl router, int port) {
		this.router = router;
		this.port = port;
		
		init();
	}

    boolean isTrue(String s) {
        return s.equals("1") ||
       		s.equals("t") ||
       		s.equals("T") ||
       		s.equals("true") ||
       		s.equals("TRUE") ||
       		s.equals("y") ||
       		s.equals("Y") ||
       		s.equals("yes") ||
       		s.equals("YES"); 
    }

    void init() {

        codecs = router.codecs();

        Element cfgNode = (Element)router.cfgXml.selectSingleNode("/parameters/ServerSos");
        if( cfgNode != null ) {

            String s = cfgNode.attributeValue("threadNum","");
            if( !s.equals("") ) threadNum = Integer.parseInt(s);

            s = cfgNode.attributeValue("host","");
            if( !s.equals("") ) host = s;

            s = cfgNode.attributeValue("timeout","");
            if( !s.equals("") ) timeout = Integer.parseInt(s);

            s = cfgNode.attributeValue("timerInterval","");
            if( !s.equals("") ) timerInterval = Integer.parseInt(s);

            s = cfgNode.attributeValue("maxPackageSize","");
            if( !s.equals("") ) maxPackageSize = Integer.parseInt(s);

            s = cfgNode.attributeValue("maxConns","");
            if( !s.equals("") ) maxConns = Integer.parseInt(s);

            s = cfgNode.attributeValue("idleTimeoutMillis","");
            if( !s.equals("") ) idleTimeoutMillis = Integer.parseInt(s);

            s = cfgNode.attributeValue("isEncrypted","");
            if( !s.equals("") ) isEncrypted = isTrue(s);

            s = cfgNode.attributeValue("shakeHandsServiceIdMsgId","");
            if( !s.equals("") ) shakeHandsServiceIdMsgId = s;

            s = cfgNode.attributeValue("reverseServiceIds","");
            if( !s.equals("") ) { 
                reverseServiceIds = s;
	        } 
            
	        hasReverseServiceIds = ( !reverseServiceIds.equals("0") );
	        router.parameters.put("reverseServiceIds",reverseServiceIds);
	
	        s = cfgNode.attributeValue("spsReportTo","");
	        if( !s.equals("") ) { 
	            router.parameters.put("spsReportTo",s);
	        } 
	
	        s = cfgNode.attributeValue("spsDisconnectNotifyTo","");
	        if( !s.equals("") ) { 
	            spsDisconnectNotifyTo = s;
	        } 
	
	        s = cfgNode.attributeValue("pushToIpPort","");
	        if( !s.equals("") ) pushToIpPort = isTrue(s);
	
	        s = cfgNode.attributeValue("pushToIp","");
	        if( !s.equals("") ) pushToIp = isTrue(s);
	
	        s = cfgNode.attributeValue("pushToAny","");
	        if( !s.equals("") ) pushToAny = isTrue(s);
	
	        s = cfgNode.attributeValue("isSps","");
	        if( !s.equals("") ) { 
	            isSps = isTrue(s);
	            router.parameters.put("isSps",isSps ? "1" : "0");
	        }
	
	        if(!isSps) {
	            isEncrypted = false;
	        } else {
	            pushToIpPort = true;
	            pushToIp = false;
	            pushToAny = false;
	        }
	
			List<Element> revList = (List<Element>)cfgNode.selectNodes("ReverseIp");
	        if( revList.size() > 0 ) {
				reverseIps = new HashSet<String>();
				for (Element t : revList) {
					String text = t.getText();
					reverseIps.add(text);
				}
	            log.info("reverseIps="+ArrayHelper.mkString(reverseIps,","));
	        }


        }

        nettyServer = new NettyServer(this,
            port,
            host,idleTimeoutMillis,maxPackageSize,maxConns);

        threadFactory = new NamedThreadFactory("serversos");
        pool = new ThreadPoolExecutor(threadNum, threadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(queueSize),threadFactory);
        pool.prestartAllCoreThreads();

        QuickTimerFunction onTimeoutFunction = new QuickTimerFunction() {
        	public void call(Object a) {
        		onTimeout(a);
        	}
        };
        
        qte = new QuickTimerEngine(onTimeoutFunction,timerInterval);

        log.info("sos started");
    }

    void closeReadChannel() {
        nettyServer.closeReadChannel();
    }

    void close() {

        if( pool != null ) {
            long t1 = System.currentTimeMillis();
            pool.shutdown();
            try {
            	pool.awaitTermination(5,TimeUnit.SECONDS);
            } catch(InterruptedException e) {
            }
            long t2 = System.currentTimeMillis();
            if( t2 - t1 > 100 )
                log.warn("long time to close sos threadpool, ts={}",t2-t1);
        }

        nettyServer.close();

        if( qte != null ) {
            qte.close();
            qte = null;
        }

        log.info("sos stopped");
    }

    void start() {
        nettyServer.start();
    }

    int[] stats(){
        return nettyServer.stats();
    }
    
    void dump() {
    	StringBuilder buff = new StringBuilder();

        buff.append("pool.size=").append(pool.getPoolSize()).append(",");
        buff.append("pool.getQueue.size=").append(pool.getQueue().size()).append(",");
        buff.append("connsAddrMap.size=").append(connsAddrMap.size()).append(",");
        buff.append("connsIpMap.size=").append(connsIpMap.size()).append(",");
        buff.append("connsIpIdxMap.size=").append(connsIpIdxMap.size()).append(",");
        buff.append("allConns.size=").append(allConns.size()).append(",");

        log.info(buff.toString());

        nettyServer.dump();

        qte.dump();
    }

    public void connected(String connId) {

        if( !hasReverseServiceIds ) return;

        if( !pushToIpPort && !pushToIp && !pushToAny ) return;

        String remoteIp = parseRemoteIp(connId);
        if( reverseIps != null ) {
            if( !reverseIps.contains(remoteIp) )  return;
        }

        connsLock.lock();
        try {

            if( pushToIpPort ) {
                connsAddrMap.put(parseRemoteAddr(connId),connId);
            }

            if( pushToAny ) {
                if(!allConns.contains(connId)) allConns.add(connId);
            }

            if( pushToIp) {
            	ArrayList<String> buff = connsIpMap.get(remoteIp);
                if( buff == null ) {
                    buff = new ArrayList<String>();
                    connsIpMap.put(remoteIp,buff);
                    connsIpIdxMap.put(remoteIp,0);
                }

                if(!buff.contains(connId)) buff.add(connId);
            }

            } finally {
                connsLock.unlock();
            }

    }

    public void disconnected(String connId) {

        if( isEncrypted ) {
            aesKeyMap.remove(connId);
        }

        if(isSps)
            notifyDisconnected(connId);

        if( !hasReverseServiceIds ) return;

        if( !pushToIpPort && !pushToIp && !pushToAny ) return;

        String remoteIp = parseRemoteIp(connId);
        if( reverseIps != null ) {
            if( !reverseIps.contains(remoteIp) ) return;
        }

        connsLock.lock();
        try {

            if( pushToIpPort ) {
                connsAddrMap.remove(parseRemoteAddr(connId));
            }

            if( pushToAny ) {
                if(allConns.contains(connId)) allConns.remove(connId);
            }

            if( pushToIp) {
            	ArrayList<String> buff = connsIpMap.get(remoteIp);
                if( buff != null ) {
                    if(buff.contains(connId)) {
                        buff.remove(connId);
                        if( buff.size() == 0 ) {
                            connsIpMap.remove(remoteIp);
                            connsIpIdxMap.remove(remoteIp);
                        }
                    }
                }
            }

            } finally {
                connsLock.unlock();
            }

    }

    public void notifyDisconnected(String connId) {
        if( spsDisconnectNotifyTo.equals("0:0") ) return;
        int sequence = generateSequence();
        String[] notifyInfo = spsDisconnectNotifyTo.split(":");
        AvenueData data = new AvenueData(
            AvenueCodec.TYPE_REQUEST,
            Integer.parseInt(notifyInfo[0]),
            Integer.parseInt(notifyInfo[1]),
            sequence,
            0,
            1,
            0,
            EMPTY_BUFFER, EMPTY_BUFFER );

        try {
            data.xhead = TlvCodec4Xhead.appendGsInfo(data.xhead,parseRemoteAddr(connId),isSps);
        } catch(Exception e) {
        }
        String requestId = "SOS"+RequestIdGenerator.nextId();
        RawRequest rr = new RawRequest(requestId, data, connId, this);
        receive(rr);
    }
    

    String selectConnId(String t) {
    		return selectConnId(t,null);
    }
    
    String selectConnId(String t,String socId) {

        String toAddr = t;
        if( toAddr == null || toAddr.equals("") || toAddr.equals("*")) {
            toAddr = socId;
        }

        if( toAddr == null || toAddr.equals("") || toAddr.equals("*")) {

            if( !pushToAny ) {
                return null;
            }

            connsLock.lock();
            try {

                if( allConns.size() == 0 ) return null;

                if( allConnsIdx < 0 || allConnsIdx >= allConns.size() ) allConnsIdx = 0;
                String connId = allConns.get(allConnsIdx);

                allConnsIdx += 1;

                return connId;

            } finally {
                connsLock.unlock();
            }

        }

        String[] toAddrTokenArray = toAddr.split(":");
        if(toAddrTokenArray.length >= 3 ) {
            return toAddr;  // treat as connId
        }

        if(toAddrTokenArray.length == 2 && pushToIpPort ) {
            connsLock.lock();
            try {
                String connId = connsAddrMap.get(toAddr); // find connId
                return connId;
            } finally {
                connsLock.unlock();
            }
        }

        if(toAddrTokenArray.length == 1 && pushToIp ) {
            String remoteIp = toAddr;
            connsLock.lock();
            try {

            	ArrayList<String> buff = connsIpMap.get(remoteIp);
                if( buff == null ) {
                    return null;
                }
                Integer idx = connsIpIdxMap.get(remoteIp);
                if( idx == null ) idx = -1;
                if( idx == -1 ) {
                    return null;
                }

                if( buff.size() == 0 ) return null;

                if( idx < 0 || idx >= buff.size() ) idx = 0;
                String connId = buff.get(idx);

                idx += 1;
                connsIpIdxMap.put(remoteIp,idx);

                return connId;

            } finally {
                connsLock.unlock();
            }
        }

        return null;
    }

	public void receive(final Object v)  {

        try{
            pool.execute( new Runnable() {
                public void run() {
                    try {
                        onReceive(v);
                    } catch(Exception e) {
                        log.error("sos receive exception v={}",v,e);
                    }
                }
            });
        } catch(RejectedExecutionException e) {
            log.error("sos queue is full");
        }

    }

    void onReceive(Object v) {

        if( v instanceof RawRequest ) {
        	RawRequest rawReq = (RawRequest)v;
            if(rawReq.sender == this )
                router.receiveRequest(rawReq);
            else
                send(rawReq,timeout);
            return;
        }

        if( v instanceof RawRequestResponseInfo ) {
        	RawRequestResponseInfo reqResInfo = (RawRequestResponseInfo)v;
            if( reqResInfo.rawReq.sender == this ) {
                if( !reqResInfo.rawReq.requestId.startsWith("SOS")) // sent by sos itself
                    reply(reqResInfo.rawRes.data,reqResInfo.rawRes.connId);
            } else {
                router.receiveResponse(reqResInfo);
            }
            return;
        }

        if( v instanceof RawResponse ) {
        	RawResponse rawRes = (RawResponse)v;
            String serviceIdMsgId = rawRes.data.serviceId + ":" + rawRes.data.msgId;
            if( !in(spsDisconnectNotifyTo,serviceIdMsgId) ) { // sent by sos itself
                reply(rawRes.data,rawRes.connId);
            }
            return;
        }

        if( v instanceof RawRequestAckInfo ) {
        	RawRequestAckInfo ackInfo = (RawRequestAckInfo)v;
        	if( ackInfo.rawReq.sender == this )
                replyAck(ackInfo.rawReq.data,ackInfo.rawReq.connId);
            else
                router.receiveAck( ackInfo );
            return;
        }
        
        if( v instanceof Request ) {
        	Request req = (Request)v;
        	send(req,timeout);
            return;
        }
        
        if( v instanceof RequestResponseInfo ) {
        	RequestResponseInfo reqResInfo = (RequestResponseInfo)v;
        	router.receiveResponse(reqResInfo);
            return;
        }

        if( v instanceof RequestAckInfo ) {
        	RequestAckInfo reqAckInfo = (RequestAckInfo)v;
        	router.receiveAck( reqAckInfo );
            return;
        }
        
        if( v instanceof SosSendTimeout ) {
        	SosSendTimeout d = (SosSendTimeout)v;

        	CacheData cachedata = dataMap.remove(d.sequence);
            if( cachedata != null ) {
                if( !cachedata.isRaw ) {
                	Request req = (Request)cachedata.data;
                	Response res = createErrorResponse(ErrorCodes.SERVICE_TIMEOUT,req);
                    receive(new RequestResponseInfo(req,res));
                } else {
                	RawRequest req = (RawRequest)cachedata.data;
                	RawResponse res = createErrorResponse(ErrorCodes.SERVICE_TIMEOUT,req);
                    receive(new RawRequestResponseInfo(req,res));
                }
            } else {
                log.error("timeout but sequence not found, seq={}",d.sequence);
            }
            return;
        }

        if( v instanceof SosSendAck ) {
        	SosSendAck d = (SosSendAck)v;

        	CacheData saved = dataMap.get(d.data.sequence); // donot remove
            if( saved != null ) {
            	if( !saved.isRaw ) {
            		Request req = (Request)saved.data;
                    receive( new RequestAckInfo(req) );
            	} else {
            		RawRequest req = (RawRequest)saved.data;
                    receive( new RawRequestAckInfo(req) );
                }
            } else {
                log.warn("receive but sequence not found, seq={}",d.data.sequence);
            }
            return;
        }

        if( v instanceof SosSendResponse ) {
        	SosSendResponse d = (SosSendResponse)v;
    	
        	CacheData saved = dataMap.remove(d.data.sequence);
            if( saved != null ) {
                saved.timer.cancel();
                if( !saved.isRaw ) {
                	Request req = (Request)saved.data;
                    TlvCodec tlvCodec = codecs.findTlvCodec(req.getServiceId());
                    if( tlvCodec != null ) {
                    	
                    	MapWithReturnCode mrc = tlvCodec.decodeResponse(req.getMsgId(),d.data.body,d.data.encoding);
                    	
                        int errorCode = d.data.code;
                        if( errorCode == 0 && mrc.ec != 0 ) {
                            errorCode = mrc.ec;

                            log.error("decode response error, serviceId="+req.getServiceId()+", msgId="+req.getMsgId());
                        }

                        Response res = new Response(errorCode,mrc.body,req);
                        res.setRemoteAddr( parseRemoteAddr(d.connId) );
                        receive(new RequestResponseInfo(req,res));
                    }
                } else {
                	RawRequest req = (RawRequest)saved.data;
                	AvenueData newdata = new AvenueData( AvenueCodec.TYPE_RESPONSE,
                        req.data.serviceId,
                        req.data.msgId,
                        req.data.sequence,
                        0,
                        req.data.encoding,
                        d.data.code,
                        d.data.xhead,
                        d.data.body
                        );
                	RawResponse res = new RawResponse (newdata,req);
                    res.remoteAddr = parseRemoteAddr(d.connId);
                    receive(new RawRequestResponseInfo(req,res));
                }
            } else {
                log.warn("receive but sequence not found, seq={}",d.data.sequence);
            }
            return;
        }
        
        log.error("unknown msg received");
    }

    boolean isAck(int code) {
    	return code == AvenueCodec.ACK_CODE; 
    }

    int generateSequence() {
        return generator.getAndIncrement();
    }

    RawResponse createErrorResponse(int code,RawRequest req) {
    	AvenueData data = new AvenueData( AvenueCodec.TYPE_RESPONSE,
            req.data.serviceId,
            req.data.msgId,
            req.data.sequence,
            0,
            req.data.encoding,
            code,
            EMPTY_BUFFER,
            EMPTY_BUFFER
            );
    	RawResponse res = new RawResponse(data,req.connId);
        return res;
    }

    Response createErrorResponse(int code,Request req) {
    	Response res = new Response(code,new HashMap<String,Object>(),req);
        return res;
    }

    void send(Request req, int timeout) { // sos->req->soc, request
        TlvCodec tlvCodec = codecs.findTlvCodec(req.getServiceId());
        if( tlvCodec == null ) {
        	Response res = createErrorResponse(ErrorCodes.TLV_ERROR,req);
            router.receiveResponse(new RequestResponseInfo(req,res));
            return;
        }

        int sequence = generateSequence();

        ByteBuffer xhead = TlvCodec4Xhead.encode(req.getServiceId(), req.getXhead());
        ByteBufferWithReturnCode ret = tlvCodec.encodeRequest(req.getMsgId(),req.getBody(),req.getEncoding());
        if( ret.ec != 0 ) {
            log.error("encode request error, serviceId="+req.getServiceId()+", msgId="+req.getMsgId());

            Response res = createErrorResponse(ret.ec,req);
            router.receiveResponse(new RequestResponseInfo(req,res));
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
            xhead, ret.bb );

        String connId = selectConnId(req.getToAddr(),(String)req.getXhead().get(AvenueCodec.KEY_SOC_ID));
        if( connId == null || connId.equals("") ) {
        	Response res = createErrorResponse(ErrorCodes.NETWORK_ERROR,req);
            router.receiveResponse(new RequestResponseInfo(req,res));
            return;
        }

        ByteBuffer bb = encode(data,connId);

        QuickTimer t = qte.newTimer(timeout,sequence);
        dataMap.put(sequence,new CacheData(false,req,t));

        boolean ok = nettyServer.write(connId,bb);

        if( !ok ) {
            dataMap.remove(sequence);
            t.cancel();
            Response res = createErrorResponse(ErrorCodes.NETWORK_ERROR,req);
            router.receiveResponse(new RequestResponseInfo(req,res));
        }

    }

    void send(RawRequest req, int timeout) { // sos->req->soc, raw request

        int sequence = generateSequence();

        HashMap<String,Object> xhead = TlvCodec4Xhead.decode(req.data.serviceId, req.data.xhead);

        String connId = selectConnId(null,(String)xhead.get(AvenueCodec.KEY_SOC_ID));
        if( connId == null || connId.equals("") ) {
        	RawResponse res = createErrorResponse(ErrorCodes.NETWORK_ERROR,req);
            router.receiveResponse(new RawRequestResponseInfo(req,res));
            return;
        }

        AvenueData data = new AvenueData(
            AvenueCodec.TYPE_REQUEST,
            req.data.serviceId,
            req.data.msgId,
            sequence,
            0,
            req.data.encoding,
            0,
            EMPTY_BUFFER, req.data.body );

        ByteBuffer bb = encode(data,connId);

        QuickTimer t = qte.newTimer(timeout,sequence);
        dataMap.put(sequence,new CacheData(true,req,t));

        boolean ok = nettyServer.write(connId,bb);

        if( !ok ) {
            dataMap.remove(sequence);
            t.cancel();
            RawResponse res = createErrorResponse(ErrorCodes.NETWORK_ERROR,req);
            router.receiveResponse(new RawRequestResponseInfo(req,res));
        }

    }

    void onTimeout(Object data)  { // called by quicktimerengine
        int sequence = (Integer)data;
        receive(new SosSendTimeout(sequence));
    }

    public void receive(ByteBuffer bb,String connId)  { // sos<-req|res<-soc, 接收请求或响应

        AvenueData data = decode(bb,connId);

        switch(data.flag) {

            case AvenueCodec.TYPE_REQUEST: {
	                if( isPing(data.serviceId, data.msgId)  ) {
	                    sendPong(data,connId);
	                    return;
	                }
	
	                // append remote addr to xhead, the last addr is always remote addr
	                try {
	                    data.xhead = TlvCodec4Xhead.appendGsInfo(data.xhead,parseRemoteAddr(connId),isSps);
	                } catch(Exception e) {
	                }
	
	                String requestId = RequestIdGenerator.nextId();
	                RawRequest rr = new RawRequest(requestId, data,connId, this);
	                receive(rr);
	            }
	            break;


            case AvenueCodec.TYPE_RESPONSE: {

	                if( isAck(data.code)  ) {
	
	                    try {
	                        receive(new SosSendAck(data,connId));
	                    } catch(Exception e){
                            log.error("receive exception res={}",data,e);
	                    }
	                    return;
	                }
	
	                // append remote addr to xhead, the last addr is always remote addr
	                try {
	                    data.xhead = TlvCodec4Xhead.appendGsInfo(data.xhead,parseRemoteAddr(connId));
	                } catch(Exception e) {
	                }
	
	                try {
	                    receive(new SosSendResponse(data,connId));
	                } catch(Exception e) {
	                        log.error("receive exception res={}",data,e);
	                }
	            	
	            }
            	break;

            default:

                log.error("unknown type");

        }

    }

    void reply(AvenueData data,String connId) { // sos->res->soc, normal response

        if( connId == null || connId.equals("") ) return;

        ByteBuffer bb = encode(data,connId);
        nettyServer.write(connId,bb);
    }

    void replyWithErrorCode(int code, AvenueData req,String  connId) {  // sos->res->soc, error code

        if( connId == null || connId.equals("") ) return;

        AvenueData res = new AvenueData (
            AvenueCodec.TYPE_RESPONSE,
            req.serviceId,
            req.msgId,
            req.sequence,
            0,
            req.encoding,
            code,
            EMPTY_BUFFER,
            EMPTY_BUFFER );

        ByteBuffer bb = AvenueCodec.encode(res);
        nettyServer.write(connId,bb);
    }

    void replyAck(AvenueData req, String connId) { // sos->res->soc, ack info

        if( connId == null || connId.equals("") ) return;

        AvenueData res = new AvenueData (
            AvenueCodec.TYPE_RESPONSE,
            req.serviceId,
            req.msgId,
            req.sequence,
            0,
            req.encoding,
            AvenueCodec.ACK_CODE, // ack
            EMPTY_BUFFER,
            EMPTY_BUFFER );

        ByteBuffer bb = AvenueCodec.encode(res);
        nettyServer.write(connId,bb);
    }

    void sendPong(AvenueData data,String connId) {

        if( connId == null || connId.equals("") ) return;

        AvenueData res = new AvenueData (
            AvenueCodec.TYPE_RESPONSE,
            0,
            0,
            data.sequence,
            0,
            data.encoding,
            0,
            EMPTY_BUFFER,
            EMPTY_BUFFER );

        ByteBuffer bb = AvenueCodec.encode(res);
        nettyServer.write(connId,bb);
    }

    
    boolean isPing(int serviceId,int msgId) { 
    	return serviceId == 0 && msgId == 0; 
    }

    String parseRemoteAddr(String connId) {
        int p = connId.lastIndexOf(":");

        if (p >= 0)
            return connId.substring(0,p);
        else
        	return "0.0.0.0:0";
    }
    
    String parseRemoteIp(String connId) {
        int p = connId.indexOf(":");

        if (p >= 0)
            return connId.substring(0,p);
        else
        	return "0.0.0.0";
    }

    boolean in(String ss,String s) {
    	String t = ",";
        if( ss == null || ss == "" ) return false;
        if( s == null || s == "" ) return true;
        return (t+ss+t).indexOf(t+s+t) >= 0;
    }

    AvenueData decode(ByteBuffer bb,String connId) {
    	AvenueData data = null;
        if( isEncrypted ) {
        		String key = aesKeyMap.get(connId);
                try {
                    data = AvenueCodec.decode(bb,key);
                } catch(Exception e){
                    log.error("decode error,service="+data.serviceId+",msgId="+data.msgId);
                    throw new RuntimeException("decpde exception ,e="+e.getMessage()); //  throw e;
                }

                String serviceIdMsgId = data.serviceId + ":" + data.msgId;
                if( key == null && !in(shakeHandsServiceIdMsgId,serviceIdMsgId) && serviceIdMsgId.equals("0:0")) { // heartbeat
                    log.error("decode error, not shakehanded,service="+data.serviceId+",msgId="+data.msgId);
                    throw new RuntimeException("not shakehanded");
                }
        } else {
            data = AvenueCodec.decode(bb);
        }
        return data;
    }

    ByteBuffer encode(AvenueData data,String connId) {
    	ByteBuffer bb = null;
        if( isEncrypted ) {
            String key = aesKeyMap.get(connId);
            String serviceIdMsgId = data.serviceId + ":" + data.msgId;
            if( key == null && !in(shakeHandsServiceIdMsgId,serviceIdMsgId)  ) {
                log.error("encode error, not shakehanded,service="+data.serviceId+",msgId="+data.msgId);
                return null;
            }
            if( in(shakeHandsServiceIdMsgId,serviceIdMsgId)  ) key = null;
            bb = AvenueCodec.encode(data,key);
        } else {
            bb = AvenueCodec.encode(data);
        }
        return bb;
    }


    static class SosSendAck { // sos->req->soc, 收到ack
    	
    	AvenueData data;
    	String connId;
    	SosSendAck(AvenueData data,String connId) {
    		this.data = data;
    		this.connId = connId;
    	}
    }
    
    static class SosSendResponse { // sos->req->soc, 收到响应
    	
    	AvenueData data;
    	String connId;
    	SosSendResponse(AvenueData data,String connId) {
    		this.data = data;
    		this.connId = connId;
    	}
    }

    static class SosSendTimeout{ // sos->req->soc, timeout
    	
    	int sequence;
    	SosSendTimeout(int sequence) {
    		this.sequence = sequence;
    	}
    }
	
	static class CacheData {
		boolean isRaw; // rawRequest or avenueRequest
		Object data;
		QuickTimer timer;
		long sendTime;
		
		CacheData(boolean isRaw,Object data, QuickTimer timer) {
			this.isRaw = isRaw;
			this.data= data;
			this.timer = timer;
			sendTime = System.currentTimeMillis();
		}
	}

}

