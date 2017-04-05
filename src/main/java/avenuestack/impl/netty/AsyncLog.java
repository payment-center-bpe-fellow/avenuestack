package avenuestack.impl.netty;

import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.*;
import java.text.SimpleDateFormat;
import java.nio.ByteBuffer;

import org.dom4j.Element;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import avenuestack.Request;
import avenuestack.Response;
import avenuestack.impl.avenue.MapWithReturnCode;
import avenuestack.impl.avenue.TlvCodec;
import avenuestack.impl.avenue.TlvCodec4Xhead;
import avenuestack.impl.util.ArrayHelper;
import avenuestack.impl.util.NamedThreadFactory;

class AsyncLogActor implements Actor {

	static Logger log = LoggerFactory.getLogger(AsyncLogActor.class);
	
	AvenueStackImpl router;

	ArrayList<String> EMPTY_ARRAYBUFFERSTRING = new ArrayList<String>();
	ArrayList<Boolean> EMPTY_ARRAYBUFFERBOOELAN = new ArrayList<Boolean>();
    HashMap<String,Object> EMPTY_MAP = new HashMap<String,Object>();

    ThreadLocal<SimpleDateFormat> f_tl = new ThreadLocal<SimpleDateFormat>() {
        public SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        }
    };

    String splitter = ",  ";
    String valueSplitter = "^_^";

    String passwordFields;
    HashSet<String> passwordFieldsSet;
    boolean asyncLogWithFieldName = true;
    int asyncLogArray = 1;
    int asyncLogMaxParamsLen = 500;

    ConcurrentHashMap<String,Logger> requestLogMap = new ConcurrentHashMap<String,Logger>();
    ConcurrentHashMap<String,Logger> csosLogMap = new ConcurrentHashMap<String,Logger>();

    HashMap<String,ArrayList<String>> requestLogCfgReq = new HashMap<String,ArrayList<String>>();
    HashMap<String,ArrayList<String>> requestLogCfgRes = new HashMap<String,ArrayList<String>>();

    int maxThreadNum = 1;
    int queueSize = 20000;
    ThreadFactory threadFactory;
    ThreadPoolExecutor pool;

    Timer timer = new Timer("asynclogstats");

    int[] reqdts = new int[] {10,50,250,1000,3000};
    int[] sosdts = new int[] {10,50,150,250,1000};

    HashMap<String,int[]> reqStat = new HashMap<String,int[]>();
    HashMap<String,int[]> sosStat = new HashMap<String,int[]>();

    ReentrantLock reqStatsLock = new ReentrantLock(false);
    ReentrantLock sosStatsLock = new ReentrantLock(false);

    Logger reqStatLog = LoggerFactory.getLogger("avenuestack.ReqStatLog");
    Logger reqSummaryLog = LoggerFactory.getLogger("avenuestack.ReqSummaryLog");
    Logger sosStatLog = LoggerFactory.getLogger("avenuestack.SosStatLog");

    ThreadLocal<SimpleDateFormat> statf_tl = new ThreadLocal<SimpleDateFormat>() {
    	public SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:00");
        }
    };

    AtomicBoolean shutdown = new AtomicBoolean();

	AsyncLogActor(AvenueStackImpl router) {
        this.router = router;
        init();
    }

    void dump() {

    	StringBuilder buff = new StringBuilder();

        buff.append("pool.size=").append(pool.getPoolSize()).append(",");
        buff.append("pool.getQueue.size=").append(pool.getQueue().size()).append(",");

        log.info(buff.toString());
    }

    String parseNodeText(String nodeName) {
    	Element e = (Element)router.cfgXml.selectSingleNode("/parameters/"+nodeName);
    	if( e == null ) return "";
    	return e.getText();
    }
    boolean isTrue(String s) {
    	return s != null && s.equals("1") || s.equals("y") || s.equals("t") || s.equals("yes") || s.equals("true");
    }
    
    void init() {
    	
    	passwordFields = parseNodeText("AsyncLogPasswordFields") ;
    	
        if( !passwordFields.equals("") ) {

            String[] ss  = passwordFields.split(",");
            passwordFieldsSet = new HashSet<String>();

            for(String s : ss ) {
                passwordFieldsSet.add(s);
            }
        }

        String s = parseNodeText("AsyncLogThreadNum");
        if( !s.equals("") ) maxThreadNum = Integer.parseInt(s);

        s = parseNodeText("AsyncLogWithFieldName");
        if( !s.equals("") ) {
            if( isTrue(s) ) asyncLogWithFieldName = true;
            else asyncLogWithFieldName = false;
        }
        
        s = parseNodeText("AsyncLogArray");
        if( !s.equals("") ) {
            asyncLogArray = Integer.parseInt(s);
            if( asyncLogArray < 1 ) asyncLogArray = 1;
        }

        s = parseNodeText("AsyncLogMaxParamsLen");
        if( !s.equals("") ) {
            asyncLogMaxParamsLen = Integer.parseInt(s);
            if( asyncLogMaxParamsLen < 100 ) asyncLogMaxParamsLen = 100;
        }

        threadFactory = new NamedThreadFactory("asynclog");
        pool = new ThreadPoolExecutor(maxThreadNum, maxThreadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(queueSize),threadFactory);
        pool.prestartAllCoreThreads();

        timer.schedule( new TimerTask() {
            public void run() {
                while(!shutdown.get()) {

                	Calendar cal = Calendar.getInstance();
                    int seconds = cal.get(Calendar.SECOND);
                    if( seconds >= 5 && seconds <= 8 ) {

                        try {
                            timer.scheduleAtFixedRate( new TimerTask() {
                                public void run() {
                                    String now = statf_tl.get().format(new Date(System.currentTimeMillis() - 60000));
                                    AsyncLogActor.this.receive(new Stats(now));
                                }
                            }, 0, 60000 );
                        } catch(Exception e) {
                        }
                        return;
                    }
                    
                    try {
                    	Thread.sleep(1000);
                    } catch(InterruptedException e) {
                    }
                }
            }
        }, 0 );

        log.info("AsyncLogActor started");
    }

    int timeToReqIdx(int ts) {
        int i = 0;
        while(i<reqdts.length) {
            if( ts <= reqdts[i] ) return i;
            i += 1;
        }
        return reqdts.length;
    }

    int timeToSosIdx(int ts) {
        int i = 0;
        while(i<sosdts.length) {
            if( ts <= sosdts[i] ) return i;
            i += 1;
        }
        return sosdts.length;
    }

    void incSosStat(String key,int ts,int code) {

        sosStatsLock.lock();
        try {

            int[] v = sosStat.get(key);
            if( v == null ) {
                v = new int[sosdts.length+4];
                sosStat.put(key,v);
            }
            int idx = timeToSosIdx(ts)+2;  // start from 2
            v[idx] += 1;

            if( code == 0 ) v[0] += 1;  // succ
            else v[1] += 1; // failed

            if( code == -10242504 ) v[v.length-1] += 1; // timeout

        } finally {
            sosStatsLock.unlock();
        }

    }

    void incReqStat(String key,int ts,int code) {

        reqStatsLock.lock();
        try {

            int[] v = reqStat.get(key);
            if( v == null ) {
                v = new int[reqdts.length+3];
                reqStat.put(key,v);
            }
            int idx = timeToReqIdx(ts) + 2;
            v[idx] += 1;

            if( code == 0 ) v[0] += 1;
            else v[1] += 1;

        } finally {
            reqStatsLock.unlock();
        }
    }
    
    void writeStats(String now) {

        // log.info("stats received")

        writeSosStats(now);
        writeReqStats(now);
    }

    void writeSosStats(String now) {

    	HashMap<String,int[]> sosMatched = new HashMap<String,int[]>();

        sosStatsLock.lock();
        try {

            for( Map.Entry<String,int[]> entry : sosStat.entrySet() ) {
            	String key = entry.getKey();
            	int[] value = entry.getValue();
            	if( key.startsWith(now) ) {
            		sosMatched.put(key,value);
            	}
            }

            for( String key : sosMatched.keySet() ) {
            	sosStat.remove(key);
            }

        } finally {
            sosStatsLock.unlock();
        }

        for( Map.Entry<String,int[]> entry : sosMatched.entrySet() ) {
        	String key = entry.getKey();
        	int[] value = entry.getValue();
            sosStatLog.info(key+",  "+ArrayHelper.mkString(value,",  "));
        }

    }

    void writeReqStats(String now) {

    	HashMap<String,int[]> reqMatched = new HashMap<String,int[]>();

        reqStatsLock.lock();
        try {

            for( Map.Entry<String,int[]> entry : reqStat.entrySet() ) {
            	String key = entry.getKey();
            	int[] value = entry.getValue();
            	if( key.startsWith(now) ) {
            		reqMatched.put(key,value);
            	}
            }

            for( String key : reqMatched.keySet() ) {
            	reqStat.remove(key);
            }
            

        } finally {
            reqStatsLock.unlock();
        }

        int[] totals = new int[3];

        for( Map.Entry<String,int[]> entry : reqMatched.entrySet() ) {
        	String key = entry.getKey();
        	int[] value = entry.getValue();

            reqStatLog.info(key+",  "+ArrayHelper.mkString(value,",  "));

            totals[0] += value[0];
            totals[0] += value[1];
            totals[1] += value[0];
            totals[2] += value[1];
        }

        if( totals[0] > 0 ) {
            int[] clients = router.sos.stats();
            reqSummaryLog.info(now+",  request["+ArrayHelper.mkString(totals,"/")+"]"+",  client["+ArrayHelper.mkString(clients,"/")+"]");
        }

    }
  
    void close() {

        shutdown.set(true);
        timer.cancel();

        long t1 = System.currentTimeMillis();

        pool.shutdown();
        
        try {
        	pool.awaitTermination(5,TimeUnit.SECONDS);
        } catch(InterruptedException e) {
        }

        writeStats(statf_tl.get().format(new Date(System.currentTimeMillis() - 60000)));
        writeStats(statf_tl.get().format(new Date(System.currentTimeMillis())));

        long t2 = System.currentTimeMillis();
        if( t2 - t1 > 100 )
            log.warn("AsyncLogActor long time to shutdown pool, ts={}",t2-t1);

        log.info("AsyncLogActor stopped");
    }

    ArrayList<String> findReqLogCfg(int serviceId,int msgId) {
    	ArrayList<String> s = requestLogCfgReq.get(serviceId+":"+msgId);
        if( s != null ) return s;
        return EMPTY_ARRAYBUFFERSTRING;
    }
    ArrayList<String> findResLogCfg(int serviceId,int msgId) {
    	ArrayList<String> s = requestLogCfgRes.get(serviceId+":"+msgId);
        if( s != null ) return s;
        return EMPTY_ARRAYBUFFERSTRING;
    }
  
    public void receive(final Object v)  {
        try{
            pool.execute( new Runnable() {
                public void run() {
                    try {
                        onReceive(v);
                    } catch(Exception e) {
                        log.error("asynclog log exception v={}",v,e);
                    }
                }
            });
        } catch(RejectedExecutionException e) {
                log.error("asynclog queue is full");
        }

    }

    Logger getRequestLog(int serviceId,int msgId)  {
    	String key = serviceId+"."+msgId;
    	Logger log = requestLogMap.get(key);
        if( log != null ) return log;
        String key2 = "avenuestack.RequestLog."+key;
        log = LoggerFactory.getLogger(key2);
        requestLogMap.put(key,log);
        return log;
    }

    Logger getCsosLog(int serviceId,int msgId) {
        String key = serviceId+"."+msgId;
        Logger log = csosLogMap.get(key);
        if( log != null ) return log;
        String key2 = "avenuestack.CsosLog."+key;
        log = LoggerFactory.getLogger(key2);
        csosLogMap.put(key,log);
        return log;
    }

    String parseConnId(String connId) {
        if( connId == null || connId.equals("") ) return connId;

        int p = connId.lastIndexOf(":");
        if( p != -1 )
            return connId.substring(0,p);
        else
        	return "";
    }

    String[] parseFirstGsInfo(HashMap<String,Object> xhead) {
    	if( xhead == null ) return new String[]{"0","0"};
        String s = (String)xhead.get("gsInfoFirst");
        if( s == null ) s = "";
        if( s.equals("") ) s = "0:0";
        String[] gsInfo = s.split(":");
        return gsInfo;
    }
    String[] parseLastGsInfo(HashMap<String,Object> xhead) {
    	if( xhead == null ) return new String[]{"0","0"};
        String s = (String)xhead.get("gsInfoLast");
        if( s == null ) s = "";
        if( s == "" ) s = "0:0";
        String[] gsInfo = s.split(":");
        return gsInfo;
    }

    String getXheadRequestId(Request req) {
        String s = (String)req.getXhead().get("uniqueId");
        if( s == null || s.equals("") ) s = "1";
        return s;
    }

    void onReceive(Object v)  {

    	if( v instanceof RequestResponseInfo ) {
    		RequestResponseInfo info = (RequestResponseInfo)v;
    		
            String now = statf_tl.get().format(new Date(info.res.getReceivedTime()));
            int ts = (int)(info.res.getReceivedTime() - info.req.getReceivedTime());

            StringBuilder keyBuff = new StringBuilder();
            keyBuff.append(now).append(",  ");

            boolean fromSosOrSoc = ( info.req.getSender() != null ) && ( info.req.getSender() instanceof Actor ); 
            if( !fromSosOrSoc ) {

                keyBuff.append(info.req.getServiceId());
                keyBuff.append(",  ");
                keyBuff.append(info.req.getMsgId());
                incSosStat(keyBuff.toString(),ts,info.res.getCode());

                Logger log = getCsosLog(info.req.getServiceId(),info.req.getMsgId());
                if( !log.isInfoEnabled() ) return;

                StringBuilder buff = new StringBuilder();

                buff.append(info.req.getRequestId()).append(splitter);
                buff.append(0).append(splitter).append(0).append(splitter);
                buff.append(info.req.getServiceId()).append(splitter).append(info.req.getMsgId()).append(splitter);
                buff.append(info.res.getRemoteAddr()).append(splitter);
                buff.append(splitter).append(splitter);

                TlvCodec codec = router.tlvCodecs.findTlvCodec(info.req.getServiceId());

                String msgName = codec.msgIdToNameMap.get(info.req.getMsgId());
                if( msgName == null ) msgName = "unknown";
                String serviceNameMsgName = codec.serviceName + "." + msgName ;
                buff.append(serviceNameMsgName).append(splitter);

                ArrayList<String> keysForReq = codec.msgKeysForReq.get(info.req.getMsgId());
                if( keysForReq == null ) keysForReq = EMPTY_ARRAYBUFFERSTRING;
                ArrayList<String> keysForRes = codec.msgKeysForRes.get(info.req.getMsgId());
                if( keysForRes == null ) keysForRes = EMPTY_ARRAYBUFFERSTRING;

                appendToBuff(keysForReq,info.req.getBody(),buff);

                buff.append(splitter);

                appendToBuffForRes(keysForRes,null,info.res.getBody(),null,buff);

                buff.append(splitter);

                buff.append(ts).append(splitter);
                buff.append(info.res.getCode());

                log.info(buff.toString());

            } else {

                keyBuff.append(info.req.getServiceId()).append(",  ");
                keyBuff.append(info.req.getMsgId()).append(",  ");
                keyBuff.append(parseConnId(info.req.getConnId()));

                incReqStat(keyBuff.toString(),ts,info.res.getCode());

                Logger log = getRequestLog(info.req.getServiceId(),info.req.getMsgId());
                if( !log.isInfoEnabled() ) return;

                StringBuilder buff = new StringBuilder();
                String[] clientInfo = parseFirstGsInfo(info.req.getXhead());
                String[] gsInfo = parseLastGsInfo(info.req.getXhead());
                buff.append(gsInfo[0]).append(splitter).append(gsInfo[1]).append(splitter);
                buff.append(clientInfo[0]).append(splitter).append(clientInfo[1]).append(splitter);
                String xappid = (String)info.req.getXhead().get("appId");
                if( xappid == null ) xappid = "0";
                String xareaid = (String)info.req.getXhead().get("areaId");
                if( xareaid == null ) xareaid = "0";
                String xsocId = (String)info.req.getXhead().get("socId");
                if( xsocId == null ) xsocId = "";
                buff.append(xappid).append(splitter).append(xareaid).append(splitter).append(xsocId).append(splitter);
                buff.append(info.req.getRequestId()).append(splitter).append(getXheadRequestId(info.req)).append(splitter);
                buff.append(info.req.getServiceId()).append(splitter).append(info.req.getMsgId()).append(splitter);
                buff.append("").append(splitter);
                Date d = new Date(info.req.getReceivedTime());
                buff.append(f_tl.get().format(d)).append(splitter).append(ts).append(splitter);
                buff.append(splitter);
                buff.append(splitter);

                appendIndex(buff,info.req.getServiceId(),info.req.getMsgId(),info.req.getBody(),info.res.getBody(),null) ;

                TlvCodec codec = router.tlvCodecs.findTlvCodec(info.req.getServiceId());

                ArrayList<String> keysForReq = null;
                ArrayList<String> keysForRes = null;

                keysForReq = codec.msgKeysForReq.get(info.req.getMsgId());
                if( keysForReq == null ) keysForReq = EMPTY_ARRAYBUFFERSTRING;
                keysForRes = codec.msgKeysForRes.get(info.req.getMsgId());
                if( keysForRes == null ) keysForRes = EMPTY_ARRAYBUFFERSTRING;

                appendToBuff(keysForReq,info.req.getBody(),buff);

                buff.append(splitter);

                appendToBuffForRes(keysForRes,null,info.res.getBody(),null,buff);

                buff.append(splitter);

                buff.append(info.res.getCode());

                log.info(buff.toString());
            }

            return;
    	}

    	if( v instanceof RawRequestResponseInfo ) {
			RawRequestResponseInfo rawInfo = (RawRequestResponseInfo)v;

            String now = statf_tl.get().format(new Date(rawInfo.rawRes.receivedTime));
            int ts = (int)(rawInfo.rawRes.receivedTime - rawInfo.rawReq.receivedTime);

            StringBuilder keyBuff = new StringBuilder();
            keyBuff.append(now).append(",  ");
            keyBuff.append(rawInfo.rawReq.data.serviceId).append(",  ");
            keyBuff.append(rawInfo.rawReq.data.msgId).append(",  ");
            keyBuff.append(parseConnId(rawInfo.rawReq.connId));

            incReqStat(keyBuff.toString(),ts,rawInfo.rawRes.data.code);

            Logger log = getRequestLog(rawInfo.rawReq.data.serviceId,rawInfo.rawReq.data.msgId);
            if( !log.isInfoEnabled() ) return;

            Request req = toReq(rawInfo.rawReq);
            Response res = toRes(req,rawInfo.rawRes);

            StringBuilder buff = new StringBuilder();
            String[] clientInfo = parseFirstGsInfo(req.getXhead());
            String[] gsInfo = parseLastGsInfo(req.getXhead());
            buff.append(gsInfo[0]).append(splitter).append(gsInfo[1]).append(splitter);
            buff.append(clientInfo[0]).append(splitter).append(clientInfo[1]).append(splitter);
            String xappid = (String)req.getXhead().get("appId");
            if( xappid == null ) xappid = "0";
            String xareaid = (String)req.getXhead().get("areaId");
            if( xareaid == null ) xareaid = "0";
            String xsocId = (String)req.getXhead().get("socId");
            if( xsocId == null ) xsocId = "";
            buff.append(xappid).append(splitter).append(xareaid).append(splitter).append(xsocId).append(splitter);
            buff.append(req.getRequestId()).append(splitter).append(getXheadRequestId(req)).append(splitter);
            buff.append(req.getServiceId()).append(splitter).append(req.getMsgId()).append(splitter);
            buff.append("").append(splitter);
            Date d = new Date(req.getReceivedTime());

            buff.append(f_tl.get().format(d)).append(splitter).append(ts).append(splitter);
            buff.append(splitter);
            buff.append(splitter);

            appendIndex(buff,req.getServiceId(),req.getMsgId(),req.getBody(),res.getBody(),null) ;

            TlvCodec codec = router.tlvCodecs.findTlvCodec(req.getServiceId());

            ArrayList<String> keysForReq = null;
            ArrayList<String> keysForRes = null;

            if( codec != null ) { 
                keysForReq = codec.msgKeysForReq.get(req.getMsgId());
                if( keysForReq == null ) keysForReq = EMPTY_ARRAYBUFFERSTRING;
                keysForRes = codec.msgKeysForRes.get(req.getMsgId());
                if( keysForRes == null ) keysForRes = EMPTY_ARRAYBUFFERSTRING;
            } else {
                keysForReq = EMPTY_ARRAYBUFFERSTRING;
                keysForRes = EMPTY_ARRAYBUFFERSTRING;
            }

            appendToBuff(keysForReq,req.getBody(),buff);

            buff.append(splitter);

            appendToBuffForRes(keysForRes,null,res.getBody(),null,buff);

            buff.append(splitter);

            buff.append(res.getCode());

            log.info(buff.toString());

            return;
    	}
    	
    	if( v instanceof Stats ) {
    		Stats stats = (Stats)v;
    		writeStats(stats.now);
    		return;
    	}
    	
        log.error("unknown msg received");
    }

    void appendIndex(StringBuilder buff,int serviceId,int msgId,HashMap<String,Object> req,HashMap<String,Object> res,HashMap<String,Object> dummy) {
    	buff.append(splitter);
        buff.append(splitter);
        buff.append(splitter);
    }

    void appendToBuff(ArrayList<String> keys,HashMap<String,Object> body,StringBuilder buff) {
        int i = 0;
        while( i < keys.size() ) {
            if( i > 0 ) buff.append(valueSplitter);

            String fieldName = keys.get(i);
            Object v = body.get(fieldName);
            if( v == null ) v = "";

            if( asyncLogWithFieldName ) {
                buff.append( fieldName ).append( ":" );
            }

            if( v != null ) {

                if( passwordFieldsSet != null && passwordFieldsSet.contains( fieldName ) ) {
                    buff.append("***");
                } else { 
                	output(buff,v);
                }
            }
            i+=1;
        }
    }

    void output(StringBuilder buff,Object v) {
    	
    	if( v == null ) return;
    	
    	if( v instanceof String ) {
    		String s = (String)v;
    		buff.append( removeCrNl(s) );
    		return;
    	}

    	if( v instanceof Number ) {
    		Number s = (Number)v;
    		buff.append( s.toString() );
    		return;
    	}
    	
    	if( v instanceof Map ) {
    		Map m = (Map)v;
    		buff.append( removeCrNl(m.toString()) );
    		return;
    	}

    	if( v instanceof List ) {
    		List l = (List)v;
    		List l2 = trimArrayList(l);
    		buff.append( removeCrNl(l2.toString()) );
    		return;
    	}
    	
    	buff.append( v.toString() );
    }

    void appendToBuffForRes(ArrayList<String> keys,ArrayList<Boolean> dummy1,HashMap<String,Object> body,HashMap<String,Object> dummy2,StringBuilder buff) {

        int i = 0;
        while( i < keys.size() ) {
            if( i > 0 ) buff.append(valueSplitter);

            String fieldName = keys.get(i);

            Object v = body.get(fieldName);
            if( v == null ) v = "";

            if( asyncLogWithFieldName ) {
                buff.append( fieldName ).append( ":" );
            }

            if( v != null ) {

                if( passwordFieldsSet != null && passwordFieldsSet.contains( fieldName ) ) {
                    buff.append("***");
                } else { 
                	output(buff,v);
                }
            }
            i+=1;
        }
        
    }

    String removeCrNl(String s) {
    	return removeSeparator(removeNl(removeCr(s)));
    }
    String removeCr(String s) {
        if( s.indexOf("\r")<0) return s;
        return s.replaceAll("\r","");
    }
    String removeNl(String s) {
        if( s.indexOf("\n")<0) return s;
        return s.replaceAll("\n","");
    }
    String removeSeparator(String s) {
        if( s.indexOf(",  ")<0) return s;
        return s.replaceAll(",  ","");
    }

    List trimArrayList(List a) {
        if( a.size() <= 1 ) return a;
        ArrayList buff = new ArrayList();
        int i = 0;
        int size = a.size();
        while( i < asyncLogArray && i < size ) {
            buff.add(a.get(i));
            i += 1;
        }
        return buff;
    }

    ByteBuffer makeCopy(ByteBuffer buff) {
        return buff.duplicate();
    }

    Response toRes(Request req, RawResponse rawRes) {

    	TlvCodec tlvCodec = router.tlvCodecs.findTlvCodec(rawRes.data.serviceId);
        if( tlvCodec != null ) {
        	ByteBuffer copiedBody = makeCopy(rawRes.data.body); // may be used by netty, must make a copy first
            MapWithReturnCode d = tlvCodec.decodeResponse(rawRes.data.msgId,copiedBody,rawRes.data.encoding);
            int errorCode = rawRes.data.code;
            if( errorCode == 0 && d.ec != 0 ) errorCode = d.ec;
            Response res = new Response (errorCode,d.body,req);
            return res;
        } else {
        	Response res = new Response (rawRes.data.code,EMPTY_MAP,req);
            return res;
        }

    }

    Request toReq(RawRequest rawReq) {

        String requestId = rawReq.requestId;

        HashMap<String,Object> xhead = EMPTY_MAP;

        try {
            xhead = TlvCodec4Xhead.decode(rawReq.data.serviceId,rawReq.data.xhead);
        } catch(Exception e) {
                log.error("decode exception in async log",e);
        }

        HashMap<String,Object>  body = EMPTY_MAP;
        TlvCodec tlvCodec = router.tlvCodecs.findTlvCodec(rawReq.data.serviceId);
        if( tlvCodec != null ) {
        	MapWithReturnCode d = tlvCodec.decodeRequest(rawReq.data.msgId,rawReq.data.body,rawReq.data.encoding);
            // ignore ec
            body = d.body;
        }

        Request req = new Request(
            requestId,
            rawReq.connId,
            rawReq.data.sequence,
            rawReq.data.encoding,
            rawReq.data.serviceId,
            rawReq.data.msgId,
            xhead,
            body,
            null
        );

        req.setReceivedTime( rawReq.receivedTime );

        return req;
    }

    
    class Stats {
    	String now;
    	Stats(String now) {
    		this.now = now;
    	}
    }

}

