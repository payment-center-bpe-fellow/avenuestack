package avenuestack.impl.netty;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import avenuestack.Request;
import avenuestack.RequestHelper;
import avenuestack.RequestReceiver;
import avenuestack.Response;
import avenuestack.ResponseReceiver;
import avenuestack.AvenueStack;
import avenuestack.ErrorCodes;
import avenuestack.impl.avenue.AvenueCodec;
import avenuestack.impl.avenue.AvenueData;
import avenuestack.impl.avenue.ByteBufferWithReturnCode;
import avenuestack.impl.avenue.MapWithReturnCode;
import avenuestack.impl.avenue.TlvCodec;
import avenuestack.impl.avenue.TlvCodec4Xhead;
import avenuestack.impl.avenue.TlvCodecs;
import avenuestack.impl.util.NamedThreadFactory;
import avenuestack.impl.util.RequestIdGenerator;

public class AvenueStackImpl implements AvenueStack {

	static Logger log = LoggerFactory.getLogger(AvenueStackImpl.class);
	HashMap<String,Object> EMPTY_MAP = new HashMap<String,Object>();
	ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
    		
	String confDir = ".";
	String profile = "prd";

	String configXml = "avenue.xml";
	String avenueConfDir = "avenue_conf";

	HashMap<String,String> parameters = new HashMap<String,String>();
	
	TlvCodecs tlvCodecs;
	Element cfgXml;
	
	RequestReceiver reqrcv;
	Sos sos;
	HashMap<String,Actor> actorMap = new HashMap<String,Actor>();
	AsyncLogActor asyncLogActor;
	
    int queueSize = 10000;
    int maxThreadNum = 1;
    ThreadFactory threadFactory;
    ThreadPoolExecutor pool;
	
	public void init() throws Exception {
		
		configXml = confDir + File.separatorChar + "avenue_"+profile+".xml";
		if( !new File(configXml).exists() ) 
			configXml = confDir + File.separatorChar + "avenue.xml";
		
		avenueConfDir = confDir + File.separatorChar + avenueConfDir;

		tlvCodecs = new TlvCodecs(avenueConfDir);
		initRequestHelper();
		
		SAXReader saxReader = new SAXReader();
		saxReader.setEncoding("UTF-8");
		cfgXml = saxReader.read(new FileInputStream(configXml)).getRootElement(); 
		
        Element cfgNode = (Element)cfgXml.selectSingleNode("/parameters/AvenueStack");
        if( cfgNode != null ) {
            String s = cfgNode.attributeValue("threadNum","");
            if( !s.equals("") ) maxThreadNum = Integer.parseInt(s);
            s = cfgNode.attributeValue("queueSize","");
            if( !s.equals("") ) queueSize = Integer.parseInt(s);
        }
        
        threadFactory = new NamedThreadFactory("sosworker");
        pool = new ThreadPoolExecutor(maxThreadNum, maxThreadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(queueSize),threadFactory);
        pool.prestartAllCoreThreads();
		
		asyncLogActor = new AsyncLogActor(this);
		
		Element n = (Element)cfgXml.selectSingleNode("/parameters/SapPort");
		if( n != null ) {
			int port = Integer.parseInt(n.getText());
			if( port > 0 ) {
				sos = new Sos(this,port);
				String[] ss = sos.reverseServiceIds.split(",");
	            for(String s:ss) {
	            	if( s != null && !s.equals("0") && !s.equals(""))
	            		actorMap.put(s, sos);
	            }
			}
		}
		
        List<Element> socList = cfgXml.selectNodes("/parameters/SosList");
        
		for (Element t : socList) {
            SocActor actor = new SocActor(this,t);
            String[] ss = actor.serviceIds.split(",");
            for(String s:ss)
            	actorMap.put(s, actor);
		}
		
	}
	
	public void initRequestHelper(){
		List<Integer> serviceIds = tlvCodecs.allServiceIds();
		for(int serviceId:serviceIds) {
			TlvCodec c = tlvCodecs.findTlvCodec(serviceId);
			String serviceName = c.serviceName;
			ArrayList<Integer> msgIds = c.allMsgIds();
			for(int msgId:msgIds) {
				String msgName = c.msgIdToName(msgId).toLowerCase();
				RequestHelper.mappings.put(serviceName+"."+msgName, serviceId+"."+msgId);
			}
		}
	}
	
	public String getConfig(String key,String defaultValue) {
		String s = parameters.get(key);
		if( s == null ) return defaultValue;
		return s;
	}
	
	public void start() {
		if( sos != null)
			sos.start();
	}
	
	public void closeReadChannel() {
		if( sos != null)
			sos.closeReadChannel();
	}

	public void close() {
		for(Actor soc: actorMap.values()) {
			if( soc instanceof SocActor ) {
				((SocActor)soc).close();
			}
		}
		sos.close();
		
        if( pool != null ) {
            long t1 = System.currentTimeMillis();
            pool.shutdown();
            try {
            	pool.awaitTermination(5,TimeUnit.SECONDS);
            } catch(InterruptedException e) {
            }
            long t2 = System.currentTimeMillis();
            if( t2 - t1 > 100 )
                log.warn("long time to close aveenuestack threadpool, ts={}",t2-t1);
        }
		
		asyncLogActor.close();
		
		log.info("avenuestack closed");
	}
	
	RawRequestResponseInfo genRawReqResInfo(RawRequest req,int code){
		AvenueData data = new AvenueData(
            AvenueCodec.TYPE_RESPONSE,req.data.serviceId,req.data.msgId,req.data.sequence,
            0,req.data.encoding,
            code,
            EMPTY_BUFFER,
            EMPTY_BUFFER);

		RawResponse res = new RawResponse(data,req.connId);
		RawRequestResponseInfo info = new RawRequestResponseInfo(req,res);
        return info;
    }
    
	// 收到网络请求
	public void receiveRequest(RawRequest rawReq) {
		
        AvenueData data = rawReq.data;
        String connId = rawReq.connId;
        //long receivedTime = rawReq.receivedTime;

        // todo 目前收到所有请求都转发给上层服务，不支持直接转发
        
        try {

            if( reqrcv == null ) {
                log.error("request receiver not defined,serviceid={}",data.serviceId);
                RawRequestResponseInfo info = genRawReqResInfo(rawReq,ErrorCodes.SERVICE_NOT_FOUND);
                asyncLogActor.receive(info);
                rawReq.sender.receive(info);
                // persistCommit( rawReq, ResultCodes.SERVICE_NOT_FOUND )
                return;
            }
        	
            HashMap<String,Object> xhead = TlvCodec4Xhead.decode(data.serviceId,data.xhead);

            TlvCodec tlvCodec = tlvCodecs.findTlvCodec(data.serviceId);
            if( tlvCodec == null ) {
                log.error("tlv codec not found,serviceid={}",data.serviceId);
                RawRequestResponseInfo info = genRawReqResInfo(rawReq,ErrorCodes.SERVICE_NOT_FOUND);
                asyncLogActor.receive(info);
                rawReq.sender.receive(info);
                // persistCommit( rawReq, ResultCodes.SERVICE_NOT_FOUND )
                return;
            }
            
            
            MapWithReturnCode d = tlvCodec.decodeRequest(data.msgId,data.body,data.encoding);
            if( d.ec != 0 ) {
                log.error("decode request error, serviceId="+data.serviceId+", msgId="+data.msgId);
                RawRequestResponseInfo info = genRawReqResInfo(rawReq,d.ec);
                asyncLogActor.receive(info);
                rawReq.sender.receive(info);
                //persistCommit( rawReq,d.ec );
                return;
            }

            final Request req = new Request (
                rawReq.requestId,
                connId,
                data.sequence,
                data.encoding,
                data.serviceId,
                data.msgId,
                xhead,
                d.body,
                rawReq.sender
            );

            req.setReceivedTime( rawReq.receivedTime );

            //if( rawReq.persistId == 0 )
              //  persist( rawReq );

            //req.persistId = rawReq.persistId;

            //if( rawReq.persistId != 0 ) {
              //  rawReq.sender.receive( new RawRequestAckInfo(rawReq) );
            //}
            
            try {
                pool.execute( new Runnable() {
                    public void run() {
                        try {
                        	reqrcv.receiveRequest(req);
                        } catch(Exception e) {
                            log.error("avenuestatck exception ,req="+req.toString() +", e="+e.getMessage(),e);
                        }
                    }
                });
            } catch(RejectedExecutionException e) {
                // ignore the message
                log.error("avenuestatck queue is full,req="+req.toString());
            }
            

        } catch(Exception e) {
                log.error("process raw request error",e);
                RawRequestResponseInfo info = genRawReqResInfo(rawReq,ErrorCodes.INTERNAL_ERROR);
                asyncLogActor.receive(info);
                rawReq.sender.receive(info);
                //persistCommit( rawReq,ResultCodes.SERVICE_INTERNALERROR );
        }
    }

	// 返回响应
	public void sendResponse(int code, HashMap<String, Object> body, Request req) {

        //persistCommit(info.req, res.code)

        TlvCodec tlvCodec = tlvCodecs.findTlvCodec(req.getServiceId());
        if( tlvCodec == null ) {
        	Response res = new Response(code,body,req);
        	RequestResponseInfo info = new RequestResponseInfo(req,res);
            asyncLogActor.receive(info);
            return;
        }

        ByteBufferWithReturnCode d = tlvCodec.encodeResponse(req.getMsgId(),body,req.getEncoding());
        int errorCode = code;
        
        Response res = new Response(code,body,req);
    	RequestResponseInfo info = new RequestResponseInfo(req,res);
    	
        if( errorCode == 0 && d.ec != 0 ) {
            log.error("encode response error, serviceId="+req.getServiceId()+", msgId="+req.getMsgId());
            errorCode = d.ec;
            info.res.setCode(d.ec);
        }

        AvenueData data = new AvenueData(
            AvenueCodec.TYPE_RESPONSE,req.getServiceId(),req.getMsgId(),req.getSequence(),
            0,req.getEncoding(),
            errorCode,
            EMPTY_BUFFER,
            d.bb);

        Actor actor = (Actor)req.getSender(); // 消息一定来源于某个actor: sos 或  soc
        actor.receive( new RawResponse(data,req.getConnId()) );
        asyncLogActor.receive(info);
	}
	
	public void sendRequest(Request req,int timeout,ResponseReceiver resrcv) {
		req.setRequestId(RequestIdGenerator.nextId());
		if(req.getXhead() == null ) req.setXhead(EMPTY_MAP);
		if(req.getBody() == null ) req.setBody(EMPTY_MAP);
		Actor actor = actorMap.get(String.valueOf(req.getServiceId()));
		if( actor == null ) {
			Response res = new Response(ErrorCodes.SERVICE_NOT_FOUND,new HashMap<String,Object>(),req);
			resrcv.receiveResponse(req, res);
			RequestResponseInfo info = new RequestResponseInfo(req,res);
			asyncLogActor.receive(info);
			return;
		}
		req.setSender(resrcv);
		actor.receive(new RequestWithTimeout(req,timeout));
	}

	public void receiveAck(RequestAckInfo ackInfo ){
		log.error("RequestAckInfo message received ignored in avenuestack");
	}

	public void receiveResponse(final RequestResponseInfo reqres) {
		asyncLogActor.receive(reqres);

        try {
            pool.execute( new Runnable() {
                public void run() {
                    try {
                		Request req = reqres.req;
                		Response res = reqres.res;
                		ResponseReceiver rcv = (ResponseReceiver)req.getSender(); // 消息一定来源于某个rcv
                		rcv.receiveResponse(req, res);
                    } catch(Exception e) {
                        log.error("avenuestatck exception, req="+reqres.req.toString()+", res="+reqres.res.toString()+", e="+e.getMessage(),e);
                    }
                }
            });
        } catch(RejectedExecutionException e) {
            // ignore the message
            log.error("avenuestatck queue is full,req="+reqres.req.toString()+",res="+reqres.res.toString());
        }
        
	}
	
	public Response sendRequestWithReturn(Request req,int timeout) {
		req.setRequestId(RequestIdGenerator.nextId());
		Actor actor = actorMap.get(String.valueOf(req.getServiceId()));
		if( actor == null ) {
			Response res = new Response(ErrorCodes.SERVICE_NOT_FOUND,new HashMap<String,Object>(),req);
			RequestResponseInfo info = new RequestResponseInfo(req,res);
			asyncLogActor.receive(info);
			return res;
		}
		
		final ReentrantLock lock = new ReentrantLock();
		final Condition responsed  = lock.newCondition(); 
		final AtomicReference<Response> r = new AtomicReference<Response>(); 
		ResponseReceiver resrcv = new ResponseReceiver() {
			public void receiveResponse(Request req,Response res) {
				lock.lock();
				try {
					r.set(res);
					responsed.signal();
				} finally {
					lock.unlock();
				}
			}
		};
		
		req.setSender(resrcv);
		lock.lock();
		try {
			actor.receive(req);
			try {
				boolean ok = responsed.await(timeout,TimeUnit.MILLISECONDS);
				if( ok ) {
					Response res = r.get();
					RequestResponseInfo info = new RequestResponseInfo(req,res);
					asyncLogActor.receive(info);
					return res;
				}
				Response res = new Response(ErrorCodes.SERVICE_TIMEOUT,new HashMap<String,Object>(),req);
				RequestResponseInfo info = new RequestResponseInfo(req,res);
				asyncLogActor.receive(info);
				return res;
			} catch(InterruptedException e) {
				Response res = new Response(ErrorCodes.NETWORK_ERROR,new HashMap<String,Object>(),req);
				RequestResponseInfo info = new RequestResponseInfo(req,res);
				asyncLogActor.receive(info);
				return res;
			}
		} finally {
			lock.unlock();
		}
		
		
	}
	
	public void receiveAck(RawRequestAckInfo ackInfo ){
		log.error("RawRequestAckInfo message should not be received in avenuestack");
	}

	public void receiveResponse(RawRequestResponseInfo reqres) {
		log.error("RawRequestResponseInfo message should not be received in avenuestack");
	}
	
	public void setRequestReceiver(RequestReceiver reqrcv) {
		this.reqrcv = reqrcv;
	}

	public TlvCodecs codecs() {
		return tlvCodecs;
	}

	public String getConfDir() {
		return confDir;
	}

	public void setConfDir(String confDir) {
		this.confDir = confDir;
	}

	public String getProfile() {
		return profile;
	}

	public void setProfile(String profile) {
		this.profile = profile;
	}
	
}

