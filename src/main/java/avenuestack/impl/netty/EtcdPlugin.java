package avenuestack.impl.netty;

import java.io.File;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.DynamicChannelBuffer;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import avenuestack.ErrorCodes;
import avenuestack.impl.util.ArrayHelper;
import avenuestack.impl.util.IpUtils;
import avenuestack.impl.util.JsonCodec;

public class EtcdPlugin {

	static Logger log = LoggerFactory.getLogger(EtcdPlugin.class);

	AvenueStackImpl router;
	Element cfgNode;
    String hosts = "";
    String basePath = "/v2/keys/services";
    int ttl = 90;
    int interval = 30;

    String profile = "default";
    Timer timer = new Timer("etcdregtimer");
    String instanceId = "";
    HashMap<String,String> sosMap = new HashMap<String,String>(); // serviceId->discoverFor
    HashMap<String,String> addrsMap = new HashMap<String,String>(); // serviceId->addrs
    HashMap<String,String> regMap = new HashMap<String,String>(); // registerAs->ip:port
    HashMap<String,Boolean> unRegOnCloseMap = new HashMap<String,Boolean>(); // registerAs->unregisterOnClose  flag
    EtcdHttpClient etcdClient = null;
    TimerTask waitForRouterTask = null;

    boolean isTrue(String s) {
    	return s != null && s.equals("1") || s.equals("y") || s.equals("t") || s.equals("yes") || s.equals("true");
    }
    
    String uuid() {
        return java.util.UUID.randomUUID().toString().replaceAll("-", "");
    }

   	public EtcdPlugin(AvenueStackImpl router,Element cfgNode) {
        this.router = router;
        this.cfgNode = cfgNode;
   	}

   	void init() throws Exception {
//long t1 = System.currentTimeMillis();    	   		
        profile = router.profile;
        String s = "";

        File instanceIdFile = new File(router.dataDir + "/instance_id");
        if( instanceIdFile.exists() ) {
            s = FileUtils.readFileToString(instanceIdFile) ;
        }

        if( s.length() > 0 )
            instanceId = s;
        else {
            instanceId = uuid();
            FileUtils.writeStringToFile(instanceIdFile,instanceId); 
        }

        Element n = (Element)cfgNode.selectSingleNode("Hosts");
		if( n != null ) {
			s = n.getText();
	        if( s.equals("") )
	            throw new Exception("ETCD hosts not configed");
	        hosts = s;
		} else {
			throw new Exception("ETCD hosts not configed");
		}

        s = cfgNode.attributeValue("basePath","");
        if(!s.equals("") ) basePath = s;

        s = cfgNode.attributeValue("ttl","");
        if(!s.equals("") ) ttl = Integer.parseInt(s);

        s = cfgNode.attributeValue("interval","");
        if(!s.equals("") ) interval = Integer.parseInt(s);

//long t2 = System.currentTimeMillis();    	   		
//log.info("eted init ts1="+(t2-t1)/1000);
		etcdClient = new EtcdHttpClient(hosts,15000,1000) ;
//long t3 = System.currentTimeMillis();    	   		
//log.info("eted init ts2="+(t3-t2)/1000);
   	}

    void close() {
        timer.cancel();
        doUnRegister(); 
        etcdClient.close();
        log.info("etcdreg plugin closed");
    }

    void saveAddrsToFile(String serviceId, String addrs) {
        String dir = router.dataDir + File.separator + "etcd";
        if( !new File(dir).exists() ) new File(dir).mkdirs() ;
        String file = dir + File.separator + serviceId;
        
        try {
        	FileUtils.writeStringToFile(new File(file),addrs) ;
        } catch(Exception e) {
        }
    }
    String loadAddrsFromFile(String serviceId) {
    	String dir = router.dataDir + File.separator + "etcd";
    	String file = dir + File.separator + serviceId;
        if( !new File(file).exists() ) return "";
        try {
	        String addrs = FileUtils.readFileToString(new File(file)) ;
	        return addrs;
        } catch(Exception e) {
        	return "";
        }
    }

    String updateXml(String xml) {


    	StringReader in = new StringReader(xml);
		SAXReader saxReader = new SAXReader();
		saxReader.setEncoding("UTF-8");
		Element cfgXml;
		
		try {
			cfgXml = saxReader.read(in).getRootElement();
		} catch(Exception e) {
			log.error("cannot read xml",e);
			return xml;
		}
		
        List<Element> sos_nodelist = cfgXml.selectNodes("/parameters/SosList");
        for( Element sos_node : sos_nodelist ) {
        	String s = sos_node.attributeValue("discoverFor","");
            if( !s.equals("") ) {
            	Element n = (Element)sos_node.selectSingleNode("ServiceId");
            	String serviceId = n.getText().split(",")[0];
                sosMap.put(serviceId,s);
            }
        }
        log.info("etcd discovery: sosMap="+sosMap.toString());    

        for( Map.Entry<String,String> entry : sosMap.entrySet() ) {
        	String serviceId = entry.getKey();
        	String discoverFor = entry.getValue();
        	
            String addrs = getEtcdAddrs(profile,discoverFor);
            if( !addrs.equals("error") ) {
                addrsMap.put(serviceId,addrs);
                saveAddrsToFile(serviceId,addrs);
            } else {
                String addrs2 = loadAddrsFromFile(serviceId);
                if( !addrs2.equals("") ){
                    addrsMap.put(serviceId,addrs);
                    log.warn("cannot get addrs from etcd,use local cache");
                } else {
                    log.warn("cannot get addrs from etcd,use config in avenuestack.xml");
                }
            }
        }
        log.info("etcd discovery: addrsMap="+addrsMap.toString()) ;       
//long t1 = System.currentTimeMillis();
        Element sapPortNode = (Element)cfgXml.selectSingleNode("/parameters/SapPort");
		if( sapPortNode != null ) {
			String sapPort = sapPortNode.getText();
			if( !sapPort.equals("") && !sapPort.equals("0") ) {
				Element serverNode = (Element)cfgXml.selectSingleNode("/parameters/Server");
				if(serverNode != null ) {
		            String registerAs = serverNode.attributeValue("registerAs","");
		            if( !registerAs.equals("")){
		                regMap.put(registerAs,getHost()+":"+getTcpPort(sapPort));
		                String s = serverNode.attributeValue("unregisterOnClose","");
		                unRegOnCloseMap.put(registerAs,isTrue(s));
		            }
				}
			}
        }
//long t2 = System.currentTimeMillis();
//log.info("etcd ts="+(t2-t1)/1000);

		/*
		List<Element> httpservers = cfgXml.selectNodes("/parameters/HttpServerCfg");
        for(Element httpserver_node : httpservers ) {
            String registerAs = httpserver_node.attributeValue("registerAs","");  
            if( !registerAs.equals("")){
                String port = httpserver_node.attributeValue("port","");
                regMap.put(registerAs,getHost()+":"+getHttpPort(port));
                String s =  httpserver_node.attributeValue("unregisterOnClose","");
                unRegOnCloseMap.put(registerAs,isTrue(s));
            }
        }
        */
        log.info("etcd registry plugin, regMap="+regMap.toString());       

        waitForRouterTask = new TimerTask() {
            public void run() {
                waitForRouter();
            }
        };

        timer.schedule(waitForRouterTask, 1*1000,1*1000);

        log.info("etcdreg plugin inited");

        String newxml = xml;
        
        for( Map.Entry<String,String> entry : sosMap.entrySet() ) {
        	String serviceId = entry.getKey();
        	String discoverFor = entry.getValue();
        	
            String addrs = addrsMap.get(serviceId);
            if( addrs != null )
                newxml = replaceXml(newxml,serviceId,discoverFor,addrs);
        }

        return newxml;
    }

    String replaceXml(String xml,String serviceId,String discoverFor,String new_addrs) {
        String tag1 = "discoverFor=\""+discoverFor+"\"";
        int p1 = xml.indexOf(tag1);
        if( p1 < 0 ) return xml;

        String tag2 = "<ServiceId>"+serviceId;
        int p2 = xml.indexOf(tag2,p1);
        if( p2 < 0 ) return xml;

        String tag3 = "</ServiceId>";
        int p3 = xml.indexOf(tag3,p2);
        if( p3 < 0 ) return xml;

        String tag4= "</SosList>";
        int p4 = xml.indexOf(tag4,p3);
        if( p4 < 0 ) return xml;

        String[] ss = new_addrs.split(",");
        ArrayList<String> addrs = new ArrayList<String>();
        for( String s : ss ) {
            addrs.add("<ServerAddr>"+s+"</ServerAddr>");
        }

        String newxml = xml.substring(0,p3+tag3.length())+"\n"+ArrayHelper.mkString(addrs,"\n")+"\n"+xml.substring(p4);
        return newxml;
    }

    void reconfig(String serviceId,String addrs) {
        try {
            Actor actor = router.getActor(serviceId);
            if( actor == null ) return;
            if( ! (actor instanceof SocActor) ) return;
            SocImpl socImpl = ((SocActor)actor).getSocImpl();
            if( socImpl == null ) return;
            NettyClient nc = socImpl.getNettyClient();
            if( nc == null ) return;
            nc.reconfig(addrs);
        } catch(Exception e) {
            log.error("cannot reconfig soc, e="+e.getMessage());
        }
    }

    void waitForRouter() {
        if(!router.isStarted()) return;
        waitForRouterTask.cancel();
        doRegister();
        timer.schedule( new TimerTask() {
            public void run() {
                refresh();
            }
        }, interval*1000,interval*1000);
    }

    void refresh() {
        doRegister();
        doDiscover();
    }

    void doDiscover() {

    	for( Map.Entry<String,String> entry : sosMap.entrySet() ) {
        	String serviceId = entry.getKey();
        	String discoverFor = entry.getValue();
        	
            String addrs = getEtcdAddrs(profile,discoverFor);
            if( !addrs.equals("error") ) {
                saveAddrsToFile(serviceId,addrs);
                String oldAddrs = addrsMap.get(serviceId);
                if( oldAddrs == null || !oldAddrs.equals(addrs) ) {
                    addrsMap.put(serviceId,addrs);
                    reconfig(serviceId,addrs);
                }
            } else {
                log.error("cannot get addrs from etcd");
            }
        }
    }

    void doRegister() {
    	
    	for( Map.Entry<String,String> entry : regMap.entrySet() ) {
        	String registerAs = entry.getKey();
        	String addr = entry.getValue();

            boolean ok = registerToEtcd(profile,registerAs,addr);
            if( !ok ) {
                log.error("cannot register service to etcd, name="+registerAs);
            }
        }
    }

    void doUnRegister() {
    	
    	for( Map.Entry<String,String> entry : regMap.entrySet() ) {
        	String registerAs = entry.getKey();
        	String addr = entry.getValue();
        	
        	if( !unRegOnCloseMap.containsKey(registerAs) || !unRegOnCloseMap.get(registerAs) ) continue;
        	
            boolean ok = unRegisterToEtcd(profile,registerAs,addr);
            if( !ok ) {
                log.error("cannot unregister service to etcd, name="+registerAs);
            }
        }
    }

    String getHost() {
        return IpUtils.localIp();
    }

    String getTcpPort(String port) {
        String env_port = System.getenv("JAVAAPP_TCP_PORT"); // used in docker
        if( env_port != null && !env_port.equals("") ) {
            return env_port;
        }
        return port;
    }
    String getHttpPort(String port) {
    	String env_port = System.getenv("JAVAAPP_HTTP_PORT"); // used in docker
        if( env_port != null && !env_port.equals("") ) {
            return env_port;
        }
        return port;
    }

    String getEtcdAddrs(String profile,String discoverFor) {
        String path = basePath +"/"+profile+"/"+discoverFor;
        ErrorCodeWithContent ecc = etcdClient.sendWithReturn("get",path,"");
        if( ecc.errorCode != 0 || log.isDebugEnabled() ) {
            log.info("etcd req: path="+path+" res: errorCode="+ecc.errorCode+",content="+ecc.content);
        }

        if(ecc.errorCode!=0) return "error";
        Map<String,Object> m = JsonCodec.parseObjectNotNull(ecc.content);
        if( m.size() == 0 ) return "error";
        if( m.containsKey("errorCode") || !"get".equals(m.get("action")) ) return "error";

        Map node = (Map)m.get("node");
        if( node == null || node.size() == 0 ) return "";
        List nodelist = (List)node.get("nodes");
        if( nodelist == null || nodelist.size() == 0 ) return "";
        HashSet<String> addrs = new HashSet<String>();

        for( Object o : nodelist ) {
        	if( o instanceof Map ) {
        		Map mm = (Map)o;
            	if(mm != null) {
            	    addrs.add((String)mm.get("value"));
            	}
        	}
        }

        ArrayList<String> addrs_list = new ArrayList<String>();
        for(String addr:addrs)  addrs_list.add(addr);
        Collections.sort(addrs_list);
        return ArrayHelper.mkString(addrs_list, ",");
    }

    boolean registerToEtcd(String profile,String registerAs,String addr) {
    	String path = basePath +"/"+profile+"/"+registerAs+"/"+instanceId +"?ttl="+ttl;
        String value = "value="+addr;
        ErrorCodeWithContent ecc = etcdClient.sendWithReturn("put",path,value);

        if( ecc.errorCode != 0 || log.isDebugEnabled() ) {
            log.info("etcd req: path="+path+",value="+value+" res: errorCode="+ecc.errorCode+",content="+ecc.content);
        }

        if(ecc.errorCode!=0) return false;
        Map<String,Object> m = JsonCodec.parseObjectNotNull(ecc.content);
        if( m.size() == 0 ) return false;
        if( m.containsKey("errorCode") || !"set".equals(m.get("action")) ) return false;

        return true;
    }

    boolean unRegisterToEtcd(String profile,String registerAs,String addr) {
        String path = basePath +"/"+profile+"/"+registerAs+"/"+instanceId;
        String value = "";
        ErrorCodeWithContent ecc = etcdClient.sendWithReturn("delete",path,value);

        if( ecc.errorCode != 0 || log.isDebugEnabled() ) {
            log.info("etcd req: path="+path+",value="+value+" res: errorCode="+ecc.errorCode+",content="+ecc.content);
        }

        if(ecc.errorCode!=0) return false;
        Map<String,Object> m = JsonCodec.parseObjectNotNull(ecc.content);
        if( m.size() == 0 ) return false;
        if( m.containsKey("errorCode") || !"delete".equals(m.get("action")) ) return false;

        return true;
    }
    
}

class ErrorCodeWithContent {
	int errorCode;
	String content;
	ErrorCodeWithContent(int errorCode,String content) {
		this.errorCode = errorCode;
		this.content = content;
	}
}

class EtcdHttpClient implements HttpClient4Netty {
	
	static Logger log = LoggerFactory.getLogger(EtcdHttpClient.class);
	
	String hosts;
    int connectTimeout = 15000;
    int timerInterval = 1000;
    int maxContentLength = 1048576;

    NettyHttpClient nettyHttpClient = null;
    AtomicInteger generator = new AtomicInteger(1);
    ConcurrentHashMap<Integer,CacheData> dataMap = new ConcurrentHashMap<Integer,CacheData>();
    String localIp = IpUtils.localIp();
    int timeout = 10000;
    String[] hostsArray;
    AtomicInteger hostsIdx = new AtomicInteger(0);

	EtcdHttpClient(String hosts,int connectTimeout,int timerInterval) {
		this.hosts = hosts;
		this.connectTimeout = connectTimeout;
		this.timerInterval = timerInterval;
		
		hostsArray = hosts.split(",");
		init();
	}

    void init() {
//long t1 = System.currentTimeMillis();
        nettyHttpClient = new NettyHttpClient(this, connectTimeout, timerInterval, maxContentLength );
//long t2 = System.currentTimeMillis();
//log.info("EtcdHttpClient ts="+(t2-t1)/1000);
        log.info("EtcdHttpClient started");
    }

    void close() {
        nettyHttpClient.close();
        log.info("EtcdHttpClient stopped");
    }

    ErrorCodeWithContent sendWithReturn(String method,String path,String body) {
    	final ReentrantLock lock = new ReentrantLock();
    	final Condition cond = lock.newCondition(); 
    	final AtomicReference<ErrorCodeWithContent> r = new AtomicReference<ErrorCodeWithContent>(); 
        CallbackFunction callback = new CallbackFunction() {
        	public void call(ErrorCodeWithContent res) {
	            lock.lock();
	            try {
	                r.set(res);
	                cond.signal();
	            } finally {
	                lock.unlock();
	            }
            }
        };

		lock.lock();
		try {
            int idx = hostsIdx.get();
            if( idx > hostsArray.length ) idx = 0;
            String host = hostsArray[idx];
            send(method,host,path,body,callback);
			try {
				boolean ok = cond.await(timeout,TimeUnit.MILLISECONDS);
				if( ok ) {
					return r.get();
				}
                skipIdx(idx);
                return new ErrorCodeWithContent(ErrorCodes.SERVICE_TIMEOUT,"");
			} catch (InterruptedException e){
                    skipIdx(idx);
                    return new ErrorCodeWithContent(ErrorCodes.NETWORK_ERROR,"");
			}
		} finally {
			lock.unlock();
		}

    }

    void skipIdx(int idx) {
        int nextIdx = idx + 1 >= hostsArray.length ? 0 : idx + 1;
        hostsIdx.compareAndSet(idx,nextIdx);
    }

    void send(String method,String host,String path,String body,CallbackFunction callback) {
    	HttpRequest httpReq = generateRequest(host,path,method,body);
        int sequence = generateSequence();
        dataMap.put(sequence,new CacheData(callback));

        nettyHttpClient.send(sequence,false,host,httpReq,timeout);
    }

    public void receive(int sequence,HttpResponse httpRes) {
    	CacheData saved = dataMap.remove(sequence);
        if( saved == null ) return;
        ErrorCodeWithContent tpl = parseResult(httpRes);
        saved.callback.call(tpl);
    }

    public void networkError(int sequence) {
    	CacheData saved = dataMap.remove(sequence);
        if( saved == null ) return;
        ErrorCodeWithContent tpl = new ErrorCodeWithContent(ErrorCodes.NETWORK_ERROR,"");
        saved.callback.call(tpl);
    }

    public void timeoutError(int sequence) {
    	CacheData saved = dataMap.remove(sequence);
        if( saved == null ) return;
        ErrorCodeWithContent tpl = new ErrorCodeWithContent(ErrorCodes.SERVICE_TIMEOUT,"");
        saved.callback.call(tpl);
    }

    int generateSequence() {
        return generator.getAndIncrement();
    }

    HttpRequest generateRequest(String host,String path,String method,String body) {

    	HttpMethod m;
    	String lm = method.toLowerCase();
        if( lm.equals("get") ) m = HttpMethod.GET;
        else if( lm.equals("put") ) m = HttpMethod.PUT;
        else if( lm.equals("post") ) m = HttpMethod.POST;
        else if( lm.equals("delete") ) m = HttpMethod.DELETE;
        else m = HttpMethod.GET;

        DefaultHttpRequest httpReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1,m,path);

        ChannelBuffer buffer= new DynamicChannelBuffer(512);
        
        try {
        	buffer.writeBytes(body.getBytes("UTF-8"));
        } catch(Exception e ){
        }

        if( log.isDebugEnabled() ) {
            log.debug("method="+method+", path="+path+", content="+body);
        }

        httpReq.setContent(buffer);
        httpReq.setHeader("Host", host); // the host include port already
        if( !lm.equals("get") ) {
            httpReq.setHeader("Content-Type", "application/x-www-form-urlencoded");
            httpReq.setHeader("Content-Length", httpReq.getContent().writerIndex());
        }
        httpReq.setHeader("User-Agent", "avenuestack etcd client");
        httpReq.setHeader("Connection", "close");

        return httpReq;
    }

    ErrorCodeWithContent parseResult(HttpResponse httpRes) {

    	HttpResponseStatus status = httpRes.getStatus();
        if( status.getCode() != 200 && status.getCode() != 201 ) {

            if( log.isDebugEnabled() ) {
                log.debug("status code={}",status.getCode());
            }

            return new ErrorCodeWithContent(ErrorCodes.NETWORK_ERROR,"");
        }

        String contentTypeStr = httpRes.getHeader("Content-Type");
        ChannelBuffer content = httpRes.getContent();
        String contentStr = content.toString(Charset.forName("UTF-8"));

        if( log.isDebugEnabled() ) {
            log.debug("contentType={},contentStr={}",contentTypeStr,contentStr);
        }

        return new ErrorCodeWithContent(0,contentStr);
    }

    interface CallbackFunction {
    	void call(ErrorCodeWithContent ecc);
    }
    
    class CacheData {
    	
    	CallbackFunction callback;
    	CacheData(CallbackFunction callback) {
    		this.callback = callback;
    	}
    }

}


