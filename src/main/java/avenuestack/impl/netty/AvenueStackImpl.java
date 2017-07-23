package avenuestack.impl.netty;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import avenuestack.AvenueStack;
import avenuestack.ErrorCodes;
import avenuestack.Request;
import avenuestack.RequestHelper;
import avenuestack.RequestReceiver;
import avenuestack.Response;
import avenuestack.ResponseReceiver;
import avenuestack.impl.avenue.AvenueCodec;
import avenuestack.impl.avenue.AvenueData;
import avenuestack.impl.avenue.BufferWithReturnCode;
import avenuestack.impl.avenue.MapWithReturnCode;
import avenuestack.impl.avenue.TlvCodec;
import avenuestack.impl.avenue.TlvCodec4Xhead;
import avenuestack.impl.avenue.TlvCodecs;
import avenuestack.impl.util.NamedThreadFactory;
import avenuestack.impl.util.RequestIdGenerator;

public class AvenueStackImpl implements AvenueStack {

	static Logger log = LoggerFactory.getLogger(AvenueStackImpl.class);
	
	static String CLASSPATH_PREFIX = "classpath:";
	static HashMap<String,Object> EMPTY_MAP = new HashMap<String,Object>();
	static ChannelBuffer EMPTY_BUFFER = ChannelBuffers.buffer(0);

    int threadNum = 1;
    int queueSize = 10000;
	String profile = "default";
	String dataDir = "./data";
	String confDir = "./conf";
	Properties parameters = new Properties();

	String avenueConfDir = "./avenue_conf";
	ArrayList<String> avenueXmlFiles;

	TlvCodecs tlvCodecs;
	Element cfgXml;

	RequestReceiver reqrcv;
	Sos sos;
	HashMap<String,Actor> actorMap = new HashMap<String,Actor>();
	AsyncLogActor asyncLogActor;
	
    ThreadFactory threadFactory;
    ThreadPoolExecutor pool;
    EtcdPlugin etcdPlugin;
	
    AtomicBoolean started = new AtomicBoolean();
    
    public AvenueStackImpl() {
    }
    
    String getParameter(String key) {
    	return parameters.getProperty(key);
    }
	
	public String getParameter(String key,String defaultValue) {
		String s = getParameter(key);
		if( s == null ) return defaultValue;
		return s;
	}
	
	public void addAvenueXml(String file) {
		avenueConfDir = null;
		if(avenueXmlFiles == null ) 
			avenueXmlFiles = new ArrayList<String>();
		avenueXmlFiles.add(file);
	}
	
	public void init() throws Exception {
//long t1 = System.currentTimeMillis();
//log.info("avenuestack initing");

    	String configXml = null;
    	String parameterFile = null;
    	
		if( profile != null && !profile.equals("default") ) {
			String t = confDir + "/" + "avenuestack_"+profile+".xml";
			if( checkExist(t) )  {
				configXml = t;
			}
		}
		if(configXml == null) {
			configXml = confDir + "/" + "avenuestack.xml";
		}
		
		if( profile != null && !profile.equals("default") )
			parameterFile = confDir + "/" + "avenuestack_"+profile+".properties";
		else
			parameterFile = confDir + "/" + "avenuestack.properties";
		
		loadParameters(parameterFile);
		
		String configXmlContent = prepareConfigFile(configXml);
//long t2 = System.currentTimeMillis();
//log.info("avenuestack ts1="+(t2-t1)/1000);
		configXmlContent = updateXml(configXmlContent);
//long t3 = System.currentTimeMillis();
//log.info("avenuestack ts2="+(t3-t2)/1000);

		if( avenueXmlFiles != null )
			tlvCodecs = new TlvCodecs(avenueXmlFiles);
		else if( avenueConfDir != null )
			tlvCodecs = new TlvCodecs(avenueConfDir);
		else
			throw new Exception("avenue xml files not specified");
		
		initRequestHelper();
		
		SAXReader saxReader = new SAXReader();
		saxReader.setEncoding("UTF-8");
		cfgXml = saxReader.read(new StringReader(configXmlContent)).getRootElement(); 
		
        Element cfgNode = (Element)cfgXml.selectSingleNode("/parameters/ThreadNum");
        if( cfgNode != null ) {
            String s = cfgNode.getText();
            if( !s.equals("") ) threadNum = Integer.parseInt(s);
        }
        cfgNode = (Element)cfgXml.selectSingleNode("/parameters/QueueSize");
        if( cfgNode != null ) {
            String s = cfgNode.getText();
            if( !s.equals("") ) queueSize = Integer.parseInt(s);

        }
        
        threadFactory = new NamedThreadFactory("sosworker");
        pool = new ThreadPoolExecutor(threadNum, threadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(queueSize),threadFactory);
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
    
    boolean loadParameters(String f) {
    	try {
    		if( f.startsWith(CLASSPATH_PREFIX)) {
    			InputStream in = AvenueStackImpl.class.getResourceAsStream(f.substring(CLASSPATH_PREFIX.length()));
    			parameters.load(in);
    			in.close();
    			
	    	} else {
	    		FileInputStream in = new FileInputStream(f);
				parameters.load(in);
				in.close();
	    	}
    		return true;
		} catch(Exception e) {
			return false;
		}
    }
    
    boolean checkExist(String f) {
    	if( f.startsWith(CLASSPATH_PREFIX)) {
    		try {
    			InputStream in = AvenueStackImpl.class.getResourceAsStream(f.substring(CLASSPATH_PREFIX.length()));
    			in.close();
    			return true;
    		} catch(Exception e) {
    			return false;
    		}
    	} else {
    		return new File(f).exists();
    	}
    }

    String prepareConfigFile(String configXml) throws Exception {

    	List<String> lines = null;
		if( configXml.startsWith(CLASSPATH_PREFIX)) {
			InputStream in = AvenueStackImpl.class.getResourceAsStream(configXml.substring(CLASSPATH_PREFIX.length()));
			lines = IOUtils.readLines(in, "UTF-8");
			in.close();
    	} else {
    		FileInputStream in = new FileInputStream(configXml);
			lines = IOUtils.readLines(in, "UTF-8");
			in.close();
    	}

        if( lines.size() > 0 ) {

            for( int i = 0; i < lines.size(); ++i ) {
                String s = lines.get(i);
                if( s.indexOf("@") >= 0 ) {
                    String ns = replaceParameter(s);
                    lines.set(i,ns);
                }
            }

        }

        StringBuilder buff = new StringBuilder();
        for( int i = 0; i < lines.size(); ++i ) {
            buff.append(lines.get(i)).append("\n");
        }
        return buff.toString();
    }

    static Pattern pReg = Pattern.compile(".*(@[0-9a-zA-Z_-]+)[ <\\]\"].*"); // allowed: @xxx< @xxx]]> @xxx[SPACE]
    static Pattern pReg2 = Pattern.compile(".*(@[0-9a-zA-Z_-]+)$"); // allowed: @xxx

    String replaceParameter(String s) {
    	String ns = s;
    	while(true) {
    		String ns2 = replaceParameterInternal(ns);
    		if( ns2.equals(ns) ) return ns;
    		ns = ns2;
    	}
    }
    
    String replaceParameterInternal(String s) {

    	Matcher matchlist = pReg.matcher(s);
        if( !matchlist.matches() ) {
            matchlist = pReg2.matcher(s);
        }
        if( !matchlist.matches() ) {
            return s;
        }

        String ns = s; 
        MatchResult	mr = matchlist.toMatchResult();
        
        int cnt = mr.groupCount();
        for( int i=1; i<=cnt; ++i ) {
        	String name = mr.group(i);
            String v = getParameter(name);
            if( v != null ) {
                ns = ns.replace(name,v);
            } else {
            	ns = ns.replace(name,"");
            }
        }
        return ns;
    }
    
    String updateXml(String xml) throws Exception {

		SAXReader saxReader = new SAXReader();
		saxReader.setEncoding("UTF-8");
		Element tXml = saxReader.read(new StringReader(xml)).getRootElement();
		
        String outputXml = xml;
        
        Element cfgNode = (Element)tXml.selectSingleNode("/parameters/EtcdRegistry");
        if( cfgNode != null ) {
        	etcdPlugin = new EtcdPlugin(this,cfgNode);
        	etcdPlugin.init();
        	outputXml = etcdPlugin.updateXml(outputXml);
        }
        
        return outputXml;
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

	public void start() {
		if( sos != null)
			sos.start();
		
		started.set(true);
	}
	
	public void closeReadChannel() {
		if( sos != null)
			sos.closeReadChannel();
	}

	public void close() {

		if( etcdPlugin != null )
			etcdPlugin.close();
		
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
            AvenueCodec.TYPE_RESPONSE,
            req.data.version,
            req.data.serviceId,req.data.msgId,req.data.sequence,
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

		if(body == null )  body = new HashMap<String, Object>();
        TlvCodec tlvCodec = tlvCodecs.findTlvCodec(req.getServiceId());
        if( tlvCodec == null ) {
        	Response res = new Response(code,body,req);
        	RequestResponseInfo info = new RequestResponseInfo(req,res);
            asyncLogActor.receive(info);
            return;
        }

        BufferWithReturnCode d = tlvCodec.encodeResponse(req.getMsgId(),body,req.getEncoding());
        int errorCode = code;
        
        Response res = new Response(code,body,req);
    	RequestResponseInfo info = new RequestResponseInfo(req,res);
    	
        if( errorCode == 0 && d.ec != 0 ) {
            log.error("encode response error, serviceId="+req.getServiceId()+", msgId="+req.getMsgId());
            errorCode = d.ec;
            info.res.setCode(d.ec);
        }

        int version = req.getVersion();
        if( version == 0 ) version = tlvCodec.version;
        AvenueData data = new AvenueData(
            AvenueCodec.TYPE_RESPONSE,
            version,
            req.getServiceId(),req.getMsgId(),req.getSequence(),
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

		if( reqres.req.getSender() instanceof SyncedResponseReceiver ) {  // 使用sendRequestWithReturn调用的，直接回调，不用worker线程池调用
			try {
        		Request req = reqres.req;
        		Response res = reqres.res;
        		ResponseReceiver rcv = (ResponseReceiver)req.getSender(); // 消息一定来源于某个rcv
        		rcv.receiveResponse(req, res);
            } catch(Exception e) {
                log.error("avenuestatck exception, req="+reqres.req.toString()+", res="+reqres.res.toString()+", e="+e.getMessage(),e);
            }
		} else {
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
        
	}
	
	class SyncedResponseReceiver implements  ResponseReceiver {
		
		ReentrantLock lock;
		Condition responsed;
		AtomicReference<Response> r;
		
		SyncedResponseReceiver(ReentrantLock lock,Condition responsed,AtomicReference<Response> r) {
			this.lock = lock;
			this.responsed = responsed;
			this.r = r;
		}
		
		public void receiveResponse(Request req,Response res) {
			lock.lock();
			try {
				r.set(res);
				responsed.signal();
			} finally {
				lock.unlock();
			}
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
		ResponseReceiver resrcv = new SyncedResponseReceiver(lock,responsed,r);
		req.setSender(resrcv);
		lock.lock();
		try {
			actor.receive(req);
			try {
				boolean ok = responsed.await(timeout,TimeUnit.MILLISECONDS);
				if( ok ) {
					Response res = r.get();
					return res;
				}
				Response res = new Response(ErrorCodes.SERVICE_TIMEOUT,new HashMap<String,Object>(),req);
				return res;
			} catch(InterruptedException e) {
				Response res = new Response(ErrorCodes.NETWORK_ERROR,new HashMap<String,Object>(),req);
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
	
	public Actor getActor(String serviceId) {
		return actorMap.get(serviceId);
	}
	
	public boolean isStarted() {
		return started.get();
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
	
	public ArrayList<String> getAvenueXmlFiles() {
		return avenueXmlFiles;
	}

	public void setAvenueXmlFiles(ArrayList<String> avenueXmlFiles) {
		this.avenueXmlFiles = avenueXmlFiles;
	}

	public String getAvenueConfDir() {
		return avenueConfDir;
	}

	public void setAvenueConfDir(String avenueConfDir) {
		this.avenueConfDir = avenueConfDir;
	}

	public Properties getParameters() {
		return parameters;
	}

	public void setParameters(Properties parameters) {
		this.parameters = parameters;
	}

	public int getThreadNum() {
		return threadNum;
	}

	public void setThreadNum(int threadNum) {
		this.threadNum = threadNum;
	}

	public int getQueueSize() {
		return queueSize;
	}

	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	public String getDataDir() {
		return dataDir;
	}

	public void setDataDir(String dataDir) {
		this.dataDir = dataDir;
	}

}

