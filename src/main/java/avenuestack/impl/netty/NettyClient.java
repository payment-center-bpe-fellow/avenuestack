package avenuestack.impl.netty;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import avenuestack.impl.avenue.TlvCodec4Xhead;
import avenuestack.impl.util.ArrayHelper;
import avenuestack.impl.util.NamedThreadFactory;
import avenuestack.impl.util.QuickTimer;
import avenuestack.impl.util.QuickTimerEngine;
import avenuestack.impl.util.QuickTimerFunction;

public class NettyClient { // with Dumpable

	static Logger log = LoggerFactory.getLogger(NettyClient.class);
	
    static AtomicInteger count = new AtomicInteger(1);
    static ConcurrentHashMap<String,String> localAddrsMap = new ConcurrentHashMap<String,String>();
	
    Soc4Netty soc;
    String addrstr;
    int connectTimeout  = 15000;
    int pingInterval = 60000;
    int maxPackageSize = 2000000;
    int connSizePerAddr = 8;
    int timerInterval = 100;
    int reconnectInterval  = 1;
    ThreadPoolExecutor bossExecutor = null;
    ThreadPoolExecutor workerExecutor = null;
    HashedWheelTimer timer  = null;
    QuickTimerEngine qte = null;
    boolean waitForAllConnected  = false;
    int waitForAllConnectedTimeout  = 60000;
    boolean connectOneByOne  = false;
    boolean reuseAddress = false;
    int startPort = -1;
    boolean isSps = false; 
    
    NioClientSocketChannelFactory factory;
    ClientBootstrap bootstrap;
    ChannelHandler channelHandler;

    String[] guids; // guid array

    AddrInfo[] addrs = null;
    ConnInfo[] allConns = null;
    HashMap<String,Channel> channelsMap = new HashMap<String,Channel>(); // map for special use
    int nextIdx = 0;
    ReentrantLock lock = new ReentrantLock(false);

    AtomicInteger portIdx;
    ConcurrentHashMap<Integer,TimeoutInfo> dataMap = new ConcurrentHashMap<Integer,TimeoutInfo>();

    AtomicBoolean connected = new AtomicBoolean();
    AtomicBoolean shutdown = new AtomicBoolean();

    NamedThreadFactory bossThreadFactory;
    NamedThreadFactory workThreadFactory;
    NamedThreadFactory timerThreadFactory;

    boolean useInternalExecutor = true;
    boolean useInternalTimer = true;
    boolean useInternalQte = true;
    int qteTimeoutFunctionId = 0;

    public NettyClient(Soc4Netty soc,String addrstr) {
    	this.soc = soc;
        this.addrstr = addrstr;
    }
    
    String uniqueAddrs(String s) {
        if( s.equals("") ) return s;
        String[] ss = s.split(",");
        LinkedHashSet<String> map = new LinkedHashSet<String>();
        for( String k : ss ) map.add(k);
        return ArrayHelper.mkString(map,",");
    }

    String addrsToString()  {
    	StringBuilder b = new StringBuilder();
        for( AddrInfo ai : addrs) {
            if( b.length() > 0 ) b.append(",");
            b.append(ai.addr+":"+ai.enabled);
        }
        return b.toString();
    }

    AddrInfo[] genAddrInfos(String s) {
    	String us = uniqueAddrs(s);
        if( us.equals("") ) return new AddrInfo[0];
        String[] ss = us.split(",");
        AddrInfo[] aa = new AddrInfo[ss.length];
        for( int i = 0 ; i < aa.length ; ++ i) aa[i] = new AddrInfo(ss[i]);
        return aa;
    }

    boolean contains(String[] ss, String s) {
    	for( String k : ss ) {
    		if( k.equals(s) ) return true;
    	}
    	return false;
    }
    
    public void reconfig(String addrstr) {
        String us = uniqueAddrs(addrstr);
        String[] us_array = null;
        if( us.equals("") ) us_array = new String[0]; 
        else us_array = us.split(",");
        
        // val add = us_array.filter(s => addrs.filter(_.addr == s).size == 0 );

        ArrayList<String> addlist = new ArrayList<String>();
        for(String s:us_array) {
        	boolean found = false;
        	for(AddrInfo ai:addrs) {
        		if( ai.addr.equals(s)) {
        			found = true;
        		}
        	}
        	if( !found ) addlist.add(s);
        }
        String[] add = ArrayHelper.toStringArray(addlist);

        lock.lock();
        try {
           AddrInfo[] new_addrs = addrs;
           ConnInfo[] new_allConns = allConns;

           if( add.length > 0 ) {
               new_addrs = new AddrInfo[addrs.length + add.length];
               for(int i = 0; i < addrs.length; ++i  ) new_addrs[i] = addrs[i];
               for(int i = 0; i < add.length; ++i ) new_addrs[addrs.length+i] = new AddrInfo(add[i]);
               new_allConns = new ConnInfo[new_addrs.length*connSizePerAddr]; // 创建新数组,只扩大不缩小
               for( ConnInfo connInfo : allConns ) {
                   int idx = connInfo.hostidx + connInfo.connidx * new_addrs.length; // 复制到新位置
                   new_allConns[idx] = connInfo;
               }
           }

           for( int idx = 0; idx < new_addrs.length ; ++ idx) {
                if( contains(us_array,new_addrs[idx].addr) ) new_addrs[idx].enabled = true;
                else new_addrs[idx].enabled = false;
           }

           for( int idx = 0; idx < new_allConns.length; ++idx ) { // 已存在的地址可能需要修改状态
        	   	if( new_allConns[idx] == null ) continue; 
                ConnInfo connInfo = new_allConns[idx];

                if( contains(us_array,connInfo.addr) ) { // 地址存在
                    if( !connInfo.enabled.get()) { // 之前被设置禁止了需重新打开；否则什么也不做

                        connInfo.enabled.set(true);

                        if( connInfo.connId == null ) { // 连接已断开
                            String[] ss = connInfo.addr.split(":");
                            String host = ss[0];
                            int port = Integer.parseInt( ss[1] );
                            ChannelFuture future = null;
                            if( startPort == -1 ) {
                                future = bootstrap.connect(new InetSocketAddress(host,port));
                            } else {
                                future = bootstrap.connect(new InetSocketAddress(host,port),new InetSocketAddress(portIdx.getAndIncrement()));
                                if( portIdx.get() >= 65535 ) portIdx.set(1025);
                            }

                            final ConnInfo fConn = new_allConns[idx];
                            future.addListener( new ChannelFutureListener() {
                                public void operationComplete(ChannelFuture future) {
                                    onConnectCompleted(future,fConn);
                                }
                            } );
                        }
                    
                    } 
                } else { // 地址不存在
                    connInfo.enabled.set(false); // 设置为false就可以，连接断开重连会检查此标志；但是若连接不断开不会自动断开
                }

           }

           if( add.length > 0 ) {
               for( int idx = 0; idx < new_allConns.length; ++idx ) { // 新增的地址需建连接
            	   if( new_allConns[idx] != null ) continue;
                   int hostidx = idx % new_addrs.length;
                   int connidx = idx / new_addrs.length;
                   String addr = new_addrs[hostidx].addr;
                   String[] ss = addr.split(":");
                   String host = ss[0];
                   int port = Integer.parseInt( ss[1] );
                   new_allConns[idx] = new ConnInfo( addr , hostidx, connidx, guids[connidx] );

                   ChannelFuture future = null;
                   if( startPort == -1 ) {
                       future = bootstrap.connect(new InetSocketAddress(host,port));
                   } else {
                       future = bootstrap.connect(new InetSocketAddress(host,port),new InetSocketAddress(portIdx.getAndIncrement()));
                       if( portIdx.get() >= 65535 ) portIdx.set(1025);
                   }

                   final ConnInfo fConn = new_allConns[idx];
                   future.addListener( new ChannelFutureListener() {
                	   public void operationComplete(ChannelFuture future) {
                           onConnectCompleted(future,fConn);
                       }
                   } );

               }

               addrs = new_addrs;
               allConns = new_allConns;
           }

        }	finally {
            lock.unlock();
        }
    }

    void dump() {

        log.info("--- addrstr="+addrsToString());

        StringBuilder buff = new StringBuilder();

        buff.append("timer.threads=").append(1).append(",");
        buff.append("bossExecutor.getPoolSize=").append(bossExecutor.getPoolSize()).append(",");
        buff.append("bossExecutor.getQueue.size=").append(bossExecutor.getQueue().size()).append(",");
        buff.append("workerExecutor.getPoolSize=").append(workerExecutor.getPoolSize()).append(",");
        buff.append("workerExecutor.getQueue.size=").append(workerExecutor.getQueue().size()).append(",");
        buff.append("channels.size=").append(allConns.length).append(",");

        int connectedCount = 0 ;
        for(ConnInfo c:allConns) {
        	if( c != null && c.ch != null ) connectedCount++;
        }

        buff.append("connectedCount=").append(connectedCount).append(",");
        buff.append("dataMap.size=").append(dataMap.size()).append(",");
        buff.append("channelsMap.size=").append(channelsMap.size()).append(",");

        log.info(buff.toString());

        qte.dump();
    }

    void init() {

        guids = new String[connSizePerAddr]; // guid array
    	addrs = genAddrInfos(addrstr);
        allConns = new ConnInfo[addrs.length*connSizePerAddr];
        portIdx = new AtomicInteger(startPort);

        
        channelHandler = new ChannelHandler(this);

        // without this line, the thread name of netty will not be changed
        ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT); // or PROPOSED

        if( bossExecutor != null || workerExecutor != null ) useInternalExecutor = false;
        if( timer != null ) useInternalTimer = false;
        
        QuickTimerFunction onTimeoutFunction = new QuickTimerFunction() {
        	public void call(Object a) {
        		onTimeout(a);
        	}
        };
        
        if( qte != null ) {
            useInternalQte = false;
            qteTimeoutFunctionId = qte.registerAdditionalTimeoutFunction(onTimeoutFunction);
            log.info("qteTimeoutFunctionId="+qteTimeoutFunctionId);
        }
        if( bossExecutor == null ) {
            bossThreadFactory = new NamedThreadFactory("socboss"+NettyClient.count.getAndIncrement());
            bossExecutor = (ThreadPoolExecutor)Executors.newCachedThreadPool(bossThreadFactory);
        }
        if( workerExecutor == null ) {
            workThreadFactory = new NamedThreadFactory("socwork"+NettyClient.count.getAndIncrement());
            workerExecutor = (ThreadPoolExecutor)Executors.newCachedThreadPool(workThreadFactory);
        }
        if( timer == null ) {
            timerThreadFactory = new NamedThreadFactory("soctimer"+NettyClient.count.getAndIncrement());
            timer = new HashedWheelTimer(timerThreadFactory,1,TimeUnit.SECONDS);
        }
        if( qte == null ) {
            qte = new QuickTimerEngine(onTimeoutFunction,timerInterval);
            qteTimeoutFunctionId = 0;
        }

        factory = new NioClientSocketChannelFactory(bossExecutor ,workerExecutor);
        bootstrap = new ClientBootstrap(factory);
        bootstrap.setPipelineFactory(new PipelineFactory());

        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("connectTimeoutMillis", connectTimeout);

        if( reuseAddress )
            bootstrap.setOption("reuseAddress", true);
        else
            bootstrap.setOption("reuseAddress", false);


        for( int connidx = 0 ; connidx < connSizePerAddr; ++connidx ) 
            guids[connidx] = java.util.UUID.randomUUID().toString().replaceAll("-", "").toUpperCase();

    }
    
    void start() {
    	
        for(int hostidx = 0; hostidx < addrs.length; ++ hostidx) {
        	
        	String addr = addrs[hostidx].addr;
            String[] ss = addr.split(":");
            String host = ss[0];
            int port = Integer.parseInt(ss[1]);

            for( int connidx  = 0 ; connidx < connSizePerAddr ; ++connidx ) {

                final int idx = hostidx + connidx * addrs.length;
                allConns[idx] = new ConnInfo( addr , hostidx, connidx, guids[connidx] );

                ChannelFuture future = null;
                if( startPort == -1 ) {
                    future = bootstrap.connect(new InetSocketAddress(host,port));
                } else {
                    future = bootstrap.connect(new InetSocketAddress(host,port),new InetSocketAddress(portIdx.getAndIncrement()));
                    if( portIdx.get() >= 65535 ) portIdx.set(1025);
                }

                future.addListener( new ChannelFutureListener() {
                    public void operationComplete(ChannelFuture future) {
                        onConnectCompleted(future,allConns[idx]);
                    }
                } );

                if( waitForAllConnected ) {

                	lock.lock();
                    try {
                    	allConns[idx].future = future;
                    	allConns[idx].futureStartTime = System.currentTimeMillis();
                    }	finally {
                    	lock.unlock();
                    }

                    // one by one
                    if( connectOneByOne )
                        future.awaitUninterruptibly(connectTimeout, TimeUnit.MILLISECONDS);

                }
            }
        }

        if( waitForAllConnected ) {

            long startTs = System.currentTimeMillis();
            long t = 0L;

            while( !shutdown.get() && !connected.get() && ( t - startTs ) < waitForAllConnectedTimeout ){

                try {
                    Thread.sleep(1000);
                } catch(Exception e) {
                        shutdown.set(true);
                }

                if( !shutdown.get() ) {

                	lock.lock();
                    try {

                        t = System.currentTimeMillis();
                        for(int i = 0 ; i < allConns.length; ++i ) {
                        	 if( !allConns[i].future.isDone() ) {
                                 if( ( t - allConns[i].futureStartTime ) >= (connectTimeout + 2000) ) {
                                     log.error("connect timeout, cancel manually, idx="+i); // sometimes connectTimeoutMillis not work!!!
                                     allConns[i].future.cancel();
                                 }
                        	 }
                        }

                    }	finally {
                    	lock.unlock();
                    }

                }

            }

            for(int i = 0 ;i <  allConns.length; ++i ) {
            	allConns[i].future = null;
            }

            long endTs = System.currentTimeMillis();
            log.info("waitForAllConnected finished, connectedCount="+connectedCount()+", channels.size="+allConns.length+", ts="+(endTs-startTs)+"ms");

        } else {

        	if( addrs.length > 0 ) {
                int maxWait = connectTimeout > 2000 ? 2000 : connectTimeout;
                long now = System.currentTimeMillis();
                long t = 0L;
                while(!connected.get() && (t - now ) < maxWait){
                	try {
                		Thread.sleep(50);
                	} catch(Exception e) {
                	}
                    t = System.currentTimeMillis();
                }
        	}

        }

        log.info("netty client started, {}, connected={}",addrsToString(),connected.get());
    }

    void close() {

        shutdown.set(true);

        if (factory != null) {

            log.info("stopping netty client {}",addrsToString());

            if( useInternalTimer )
                timer.stop();
            timer = null;

            DefaultChannelGroup allChannels = new DefaultChannelGroup("netty-client-java");
            for(ConnInfo connInfo : allConns ) {
            	if( connInfo.ch != null && connInfo.ch.isOpen() ) {
            		allChannels.add(connInfo.ch);	
            	}
            }
            ChannelGroupFuture future = allChannels.close();
            future.awaitUninterruptibly();

            if( useInternalExecutor )
                factory.releaseExternalResources();
            factory = null;
        }

        if( useInternalQte )
            qte.close();

        log.info("netty client stopped {}",addrsToString());
    }
/*
    def selfcheck() : ArrayBuffer[SelfCheckResult] = {

        val buff = new ArrayBuffer[SelfCheckResult]()

        var errorId = 65301001

        var i = 0
        while( i < addrs.size ) {
            if( channels(i) == null ) {
                val msg = "sos ["+addrs(i)+"] has error"
                buff += new SelfCheckResult("JVMDBBRK.SOS",errorId,true,msg)
            }

            i += 1
        }

        if( buff.size == 0 ) {
            buff += new SelfCheckResult("JVMDBBRK.SOS",errorId)
        }

        buff
    }
*/
    void reconnect(final ConnInfo connInfo) {

        if( !connInfo.enabled.get() ) {
            log.info("reconnect disabled, hostidx={},connidx={}",connInfo.hostidx,connInfo.connidx);
            return;
        }
        
        String[] ss = connInfo.addr.split(":");
        String host = ss[0];
        int port = Integer.parseInt(ss[1]);

        log.info("reconnect called, hostidx={},connidx={}",connInfo.hostidx,connInfo.connidx);

        ChannelFuture future = null;
        if( startPort == -1 ) {
            future = bootstrap.connect(new InetSocketAddress(host,port));
        } else {
            future = bootstrap.connect(new InetSocketAddress(host,port),new InetSocketAddress(portIdx.getAndIncrement()));
            if( portIdx.get() >= 65535 ) portIdx.set(1);
        }

        future.addListener( new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) {
                onConnectCompleted(future,connInfo);
            }
        } );

        if( waitForAllConnected ) {

        	lock.lock();
            try {
                connInfo.future = future;
                connInfo.futureStartTime = System.currentTimeMillis();
            }	finally {
            	lock.unlock();
            }

        }

    }

    int connectedCount() {

        lock.lock();

        try {

            int i = 0;
            int cnt = 0;
            while(  i < allConns.length ) {
                Channel ch = allConns[i].ch;
                if( ch != null ) {
                    cnt +=1;
                }
                i+=1;
            }

            return cnt;

        } finally {
            lock.unlock();
        }

    }

    void onConnectCompleted(ChannelFuture f,final ConnInfo connInfo) {

        if (f.isCancelled()) {

            log.error("connect cancelled, hostidx="+connInfo.hostidx+",connidx="+connInfo.connidx);

            if( timer != null ) { // while shutdowning
                timer.newTimeout( new TimerTask() {

                    public void run( Timeout timeout) {
                        reconnect(connInfo);
                    }

                }, reconnectInterval, TimeUnit.SECONDS);
            }

        } else if (!f.isSuccess()) {

            log.error("connect failed, hostidx="+connInfo.hostidx+",connidx="+connInfo.connidx+",e="+f.getCause().getMessage());

            if( timer != null ) { // while shutdowning
                timer.newTimeout( new TimerTask() {

                    public void run(Timeout timeout) {
                        reconnect(connInfo);
                    }

                }, reconnectInterval, TimeUnit.SECONDS);
            }
        } else {

            Channel ch = f.getChannel();
            Channel ignore_ch = null;
            
            lock.lock();

            try {
                if( connInfo.ch == null ) {
                    String theConnId = parseIpPort(ch.getRemoteAddress().toString()) + ":" + ch.getId();
                    connInfo.ch = ch;
                    connInfo.connId = theConnId;
                    log.info("connect ok, hostidx="+connInfo.hostidx+",connidx="+connInfo.connidx+",channelId="+ch.getId()+",channelAddr="+connInfo.addr+",clientAddr="+ch.getLocalAddress().toString());
                } else {
                    log.info("connect ignored, hostidx="+connInfo.hostidx+",connidx="+connInfo.connidx+",channelId="+ch.getId()+",channelAddr="+connInfo.addr+",clientAddr="+ch.getLocalAddress().toString());
                    ignore_ch = ch;
                    ch = null;
                }
            } finally {
                lock.unlock();
            }

            if( waitForAllConnected ) {

                if( connectedCount() == allConns.length ) {
                    connected.set(true);
                }

            } else {
                connected.set(true);
            }

            if( ch == null ) {
                if( ignore_ch != null ) ignore_ch.close();
            } else {
            	soc.connected(connInfo.connId,connInfo.addr,connInfo.connidx);
            	
                String ladr = ch.getLocalAddress().toString();
                if( NettyClient.localAddrsMap.contains(ladr) ) {
                    log.warn("client addr duplicated, " + ladr );
                }	else {
                    NettyClient.localAddrsMap.put(ladr,"1");
                }

                if( isSps ) {
                	ChannelBuffer buff = soc.generateReportSpsId();
                    if( buff != null ) {
                        updateSpsId(buff,connInfo);
                        ch.write(buff);
                    }
                }
            	
            }
        }

    }

    void closeChannelFromOutside(String theConnId) {

        lock.lock();

        try {
            Channel channel = channelsMap.get(theConnId);
            if( channel != null && channel.isOpen() ) {
                channel.close();
                return ;
            }

        } finally {
            lock.unlock();
        }
    }

    void addChannelToMap(String theConnId) {

        lock.lock();

        try {
            int i = 0;

            while( i < allConns.length ) {

                Channel channel = allConns[i].ch;
                String connId = allConns[i].connId;
                if( channel != null && channel.isOpen() && connId.equals(theConnId) ) {
                    channelsMap.put(connId,channel);
                    return ;
                }

                i+=1;
            }

        } finally {
            lock.unlock();
        }

    }
    
    String selectChannel() {

        lock.lock();

        try {
            int i = 0;

            while( i < allConns.length ) {

                Channel channel = allConns[i].ch;
                String connId = allConns[i].connId;
                if( channel != null && channel.isOpen() && !channelsMap.containsKey(connId) ) {
                    channelsMap.put(connId,channel);
                    return connId;
                }

                i+=1;
            }

            return null;

        } finally {
            lock.unlock();
        }

    }
   
    Channel nextChannelFromMap(int sequence,int timeout,String connId) {

        lock.lock();

        try {

            Channel ch = channelsMap.get(connId);
            if( ch == null ) return null;

            if( ch.isOpen() ) {
            	QuickTimer t = qte.newTimer(timeout,sequence,qteTimeoutFunctionId);
            	TimeoutInfo ti = new TimeoutInfo(sequence,connId,t);
                dataMap.put(sequence,ti);
                return ch;
            } else {
                log.error("channel not opened, connId={}",connId);
                removeChannel(connId);
                return null;
            }

        } finally {
            lock.unlock();
        }
    }
    
    class ChannelAndIdx {
    	ConnInfo connInfo;
    	Channel ch;
    	int idx;
    	ChannelAndIdx(ConnInfo connInfo,Channel ch,int idx) {
    		this.connInfo = connInfo;
    		this.ch = ch;
    		this.idx = idx;
    	}
    }

    ChannelAndIdx nextChannel(int sequence,int timeout) {
    		return nextChannel(sequence,timeout,null);
    }
    ChannelAndIdx nextChannel(int sequence,int timeout,String addr) {

        lock.lock();

        try {

            int i = 0;
            while(  i < allConns.length ) {
            	ConnInfo connInfo = allConns[nextIdx];
                Channel ch = connInfo.ch;
                String connId = connInfo.connId;
                String chAddr = connInfo.addr;
                if( ch != null ) { // && ch.isWritable

                    if( ch.isOpen() ) {

                        boolean matchAddr = true;
                        if( addr != null && !chAddr.equals(addr) ) { // change to domain name match
                            matchAddr = false;
                        }

                        if( matchAddr ) {
                        	QuickTimer t = qte.newTimer(timeout,sequence,qteTimeoutFunctionId);
                        	TimeoutInfo ti = new TimeoutInfo(sequence,connId,t);
                            dataMap.put(sequence,ti);

                            ChannelAndIdx d = new ChannelAndIdx(connInfo,ch,nextIdx);
                            nextIdx += 1;
                            if( nextIdx >= allConns.length ) nextIdx = 0;
                            return d;

                        }

                        } else {
                            log.error("channel not opened, idx={}, connId={}",i,connId);
                            removeChannel(connId);
                        }

                }
                i+=1;
                nextIdx += 1;
                if( nextIdx >= allConns.length ) nextIdx = 0;
            }

            return new ChannelAndIdx(null,null,0);

        } finally {
            lock.unlock();
        }
    }
  
    void removeChannel(String connId) {

        if( shutdown.get()) {
            return;
        }

        lock.lock();

        ConnInfo connInfo = null;
        try {
        	int i = 0;

            while( connInfo == null && i < allConns.length ) {
                Channel channel = allConns[i].ch;
                String theConnId = allConns[i].connId;
                if( channel != null && theConnId.equals(connId) ) {
                	connInfo = allConns[i];
                	soc.disconnected(connInfo.connId,connInfo.addr,connInfo.connidx);
                	connInfo.ch = null;
                	connInfo.connId = null;
                }

                i+=1;
            }

            channelsMap.remove(connId);

        } finally {
            lock.unlock();
        }

        if( connInfo != null ) {
        	
        	final ConnInfo t = connInfo;
            timer.newTimeout( new TimerTask() {

                public void run( Timeout timeout) {
                    reconnect(t);
                }

            }, reconnectInterval, TimeUnit.SECONDS);
        }

    }
    
 
    boolean send(int sequence,ChannelBuffer buff,int timeout) {

    	ChannelAndIdx ci = nextChannel(sequence,timeout);

        if( ci.ch == null ) {
            return false;
        }

        if( isSps ) updateSpsId(buff,ci.connInfo);
        ci.ch.write(buff);

        return true;
    }

    boolean sendByAddr(int sequence,ChannelBuffer buff ,int timeout,String addr) {

    	ChannelAndIdx ci = nextChannel(sequence,timeout,addr);

        if( ci.ch == null ) {
            return false;
        }

        if( isSps ) updateSpsId(buff,ci.connInfo);
        ci.ch.write(buff);

        return true;
    }

    void updateSpsId(ChannelBuffer buff, ConnInfo connInfo) {
        TlvCodec4Xhead.updateSpsId(buff,connInfo.guid);
    }

    boolean sendByConnId(int sequence, ChannelBuffer buff ,int timeout,String connId) {

        Channel ch = nextChannelFromMap(sequence,timeout,connId);

        if( ch == null ) {
            return false;
        }

        ch.write(buff);

        return true;
    }

    boolean sendResponse(int sequence,ChannelBuffer buff,String connId) {

    	Channel ch = null;

        lock.lock();

        try {
            int i = 0;

            while( i < allConns.length && ch == null ) {
                Channel channel = allConns[i].ch;
                String theConnId = allConns[i].connId;
                if( channel != null && theConnId.equals(connId) ) {
                    ch = allConns[i].ch;
                }

                i+=1;
            }
        } finally {
            lock.unlock();
        }

        if( ch == null ) {
            return false;
        }

        ch.write(buff);

        return true;
    }

    String parseIpPort(String s) {

        int p = s.indexOf("/");

        if (p >= 0)
            return s.substring(p + 1);
        else
        	return s;
    }

    void onTimeout(Object data) {

        int sequence = (Integer)data;

        TimeoutInfo ti = dataMap.remove(sequence);
        if( ti != null ) {
            soc.timeoutError(sequence,ti.connId);
        } else {
            log.error("timeout but sequence not found, seq={}",sequence);
        }

    }

    void onReceive(ChannelBuffer buff,String connId) {

    	Soc4NettySequenceInfo si = soc.receive(buff,connId);

        if( si.ok ) {

            //log.info("onReceive,seq="+sequence);

        	TimeoutInfo ti = dataMap.remove(si.sequence);
            if( ti != null ) {
                ti.timer.cancel();
            } else {
                log.warn("receive but sequence not found, seq={}",si.sequence);
            }

        }

    }

    void onNetworkError(String connId) {

        removeChannel(connId);

        ArrayList<Integer> seqs = new ArrayList<Integer>();
        for(TimeoutInfo info: dataMap.values()) {
            if( info.connId.equals(connId)) {
                seqs.add(info.sequence);
            }
        }

        for(int sequence :seqs) {
        	TimeoutInfo ti = dataMap.remove(sequence);
            if( ti != null ) {
                ti.timer.cancel();
                soc.networkError(sequence,connId);
            } else {
                log.error("network error but sequence not found, seq={}",sequence);
            }
        }

    }

    public void messageReceived(ChannelHandlerContext ctx,MessageEvent e){
        //Channel ch = e.getChannel();
        String connId = (String)ctx.getAttachment();
        ChannelBuffer buff = (ChannelBuffer)e.getMessage();
        onReceive(buff,connId);
    }

    public void channelConnected(ChannelHandlerContext ctx,ChannelStateEvent e) {
    	Channel ch = e.getChannel();
        String connId = parseIpPort(ch.getRemoteAddress().toString()) + ":" + ch.getId();
        ctx.setAttachment(connId);
    }

    public void channelDisconnected(ChannelHandlerContext ctx,ChannelStateEvent e) {
    	//Channel ch = e.getChannel();
        String connId = (String)ctx.getAttachment();
        onNetworkError(connId);
        log.info("channelDisconnected id={}",connId);
    }

    public void exceptionCaught(ChannelHandlerContext ctx,ExceptionEvent e) {
    	Channel ch = e.getChannel();
        String connId = (String)ctx.getAttachment();
        log.error("exceptionCaught connId={},e={}",connId,e);
        if( ch.isOpen() )
            ch.close();
    }

    public void channelIdle(ChannelHandlerContext ctx,IdleStateEvent e) {
    	Channel ch = e.getChannel();
    	ChannelBuffer buff = soc.generatePing();
        ch.write(buff);
    }

    class PipelineFactory implements ChannelPipelineFactory {

    	public ChannelPipeline getPipeline() {
    		ChannelPipeline pipeline = Channels.pipeline();
    		LengthFieldBasedFrameDecoder decoder = new LengthFieldBasedFrameDecoder(maxPackageSize, 4, 4, -8, 0);
            pipeline.addLast("timeout", new IdleStateHandler(timer, 0, 0, pingInterval / 1000));
            pipeline.addLast("decoder", decoder);
            pipeline.addLast("handler", channelHandler);
            return pipeline;
        }
    }

    class ChannelHandler extends IdleStateAwareChannelHandler {

    	NettyClient nettyClient;
    	
    	ChannelHandler( NettyClient nettyClient) {
    		this.nettyClient = nettyClient;
    	}
    	
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            nettyClient.messageReceived(ctx,e);
        }

        public void  channelIdle(ChannelHandlerContext ctx, IdleStateEvent e)  {
            nettyClient.channelIdle(ctx,e);
        }

        public void  exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e )  {
            nettyClient.exceptionCaught(ctx,e);
        }

        public void  channelConnected(ChannelHandlerContext ctx,ChannelStateEvent  e) {
            nettyClient.channelConnected(ctx,e);
        }

        public void  channelDisconnected(ChannelHandlerContext ctx,ChannelStateEvent  e)  {
            nettyClient.channelDisconnected(ctx,e);
        }

    }

    class TimeoutInfo {
    	
    	int sequence;
    	String connId;
    	QuickTimer timer;
    	
    	TimeoutInfo(int sequence, String connId, QuickTimer timer) {
    		this.sequence = sequence;
    		this.connId = connId;
    		this.timer = timer;
    	}
    	
    }

    class AddrInfo {
    	String addr;
    	boolean enabled = true; 
    		
    	AddrInfo(String addr) {
    		this.addr = addr;
    	}
    	
    	AddrInfo(String addr,boolean enabled) {
    		this.addr = addr;
    		this.enabled = enabled;
    	}
    
}

    class ConnInfo {
    	
    	String addr;
    	int hostidx;
    	int connidx;
    	String guid;
    	
    	AtomicBoolean enabled = new AtomicBoolean(true);
        Channel ch = null;
        String connId = null;
        ChannelFuture future = null;
        long futureStartTime = 0L;
        
        ConnInfo(String addr, int hostidx, int connidx, String guid) {
        	this.addr = addr;
        	this.hostidx = hostidx;
        	this.connidx = connidx;
        	this.guid = guid;
        }
    }
    
}

