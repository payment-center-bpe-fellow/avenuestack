package avenuestack.impl.netty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.nio.ByteBuffer;
import java.net.InetSocketAddress;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.timeout.*;
import org.jboss.netty.bootstrap.*;
import org.jboss.netty.channel.group.*;
import org.jboss.netty.channel.socket.nio.*;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    String[] addrs;
    ConcurrentHashMap<Integer,TimeoutInfo> dataMap = new ConcurrentHashMap<Integer,TimeoutInfo>();

    String[] channelAddrs; // domain name array
    Channel[] channels; // channel array
    String[] channelIds; // connId array
    HashMap<String,Channel> channelsMap = new HashMap<String,Channel>(); // map for special use
    ReentrantLock lock = new ReentrantLock(false);

    String[] guids; // guid array

    int nextIdx = 0;
    AtomicBoolean connected = new AtomicBoolean();

    AtomicBoolean shutdown = new AtomicBoolean();

    NamedThreadFactory bossThreadFactory;
    NamedThreadFactory workThreadFactory;
    NamedThreadFactory timerThreadFactory;

    boolean useInternalExecutor = true;
    boolean useInternalTimer = true;
    boolean useInternalQte = true;
    int qteTimeoutFunctionId = 0;

    AtomicInteger portIdx;

    ChannelFuture[] futures;
    long[] futuresStartTime;
    ReentrantLock futureLock = new ReentrantLock(false);

    void dump() {

        log.info("--- addrstr="+addrstr);

        StringBuilder buff = new StringBuilder();

        buff.append("timer.threads=").append(1).append(",");
        buff.append("bossExecutor.getPoolSize=").append(bossExecutor.getPoolSize()).append(",");
        buff.append("bossExecutor.getQueue.size=").append(bossExecutor.getQueue().size()).append(",");
        buff.append("workerExecutor.getPoolSize=").append(workerExecutor.getPoolSize()).append(",");
        buff.append("workerExecutor.getQueue.size=").append(workerExecutor.getQueue().size()).append(",");
        buff.append("channels.size=").append(channels.length).append(",");

        int connectedCount = 0 ;
        for(Channel c:channels) {
        	if( c != null ) connectedCount++;
        }

        buff.append("connectedCount=").append(connectedCount).append(",");
        buff.append("dataMap.size=").append(dataMap.size()).append(",");

        log.info(buff.toString());

        qte.dump();
    }

    void init() {

        addrs = addrstr.split(",");
        channelAddrs = new String[addrs.length*connSizePerAddr]; // domain name array
        channels = new Channel[addrs.length*connSizePerAddr]; // channel array
        channelIds = new String[addrs.length*connSizePerAddr]; // connId array
        guids = new String[connSizePerAddr]; // guid array

        portIdx = new AtomicInteger(startPort);

        futures = new ChannelFuture[addrs.length*connSizePerAddr];
        futuresStartTime = new long[addrs.length*connSizePerAddr];
        
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
            bootstrap.setOption("reuserAddress", true);
        else
            bootstrap.setOption("reuserAddress", false);


        for( int connidx = 0 ; connidx < connSizePerAddr; ++connidx ) 
            guids[connidx] = java.util.UUID.randomUUID().toString().replaceAll("-", "").toUpperCase();

        for(int hostidx = 0; hostidx < addrs.length; ++ hostidx) {

            String[] ss = addrs[hostidx].split(":");
            String host = ss[0];
            int port = Integer.parseInt(ss[1]);

            for( int connidx  = 0 ; connidx < connSizePerAddr ; ++connidx ) {

                int idx = hostidx + connidx * addrs.length;
                channelAddrs[idx] = addrs[hostidx];

                ChannelFuture future = null;
                if( startPort == -1 ) {
                    future = bootstrap.connect(new InetSocketAddress(host,port));
                } else {
                    future = bootstrap.connect(new InetSocketAddress(host,port),new InetSocketAddress(portIdx.getAndIncrement()));
                    if( portIdx.get() >= 65535 ) portIdx.set(1025);
                }
                
                final int f_hostidx = hostidx;
                final int f_connidx = connidx;
                future.addListener( new ChannelFutureListener() {
                    public void operationComplete(ChannelFuture future) {
                        onConnectCompleted(future,f_hostidx,f_connidx);
                    }
                } );

                if( waitForAllConnected ) {

                    futureLock.lock();
                    try {
                        //val idx = hostidx + connidx * addrs.size
                        futures[idx] = future;
                        futuresStartTime[idx] = System.currentTimeMillis();
                        }	finally {
                            futureLock.unlock();
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

                    futureLock.lock();
                    try {

                        t = System.currentTimeMillis();
                        for(int i = 0 ; i < futures.length; ++i ) {
                        	 if( !futures[i].isDone() ) {
                                 if( ( t - futuresStartTime[i] ) >= (connectTimeout + 2000) ) {
                                     log.error("connect timeout, cancel manually, idx="+i); // sometimes connectTimeoutMillis not work!!!
                                     futures[i].cancel();
                                 }
                        	 }
                        }

                        }	finally {
                            futureLock.unlock();
                        }

                }

            }

            for(int i = 0 ;i <  futures.length; ++i ) {
                futures[i] = null;
            }

            long endTs = System.currentTimeMillis();
            log.info("waitForAllConnected finished, connectedCount="+connectedCount()+", channels.size="+channels.length+", ts="+(endTs-startTs)+"ms");

        } else {

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

        log.info("netty client started, {}, connected={}",addrstr,connected.get());
    }

    void close() {

        shutdown.set(true);

        if (factory != null) {

            log.info("stopping netty client {}",addrstr);

            if( useInternalTimer )
                timer.stop();
            timer = null;

            DefaultChannelGroup allChannels = new DefaultChannelGroup("netty-client-java");
            for(Channel ch : channels ) {
            	if( ch != null && ch.isOpen() ) {
            		allChannels.add(ch);	
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

        log.info("netty client stopped {}",addrstr);
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
    void reconnect(final int hostidx,final int connidx) {

        String[] ss = addrs[hostidx].split(":");
        String host = ss[0];
        int port = Integer.parseInt(ss[1]);

        log.info("reconnect called, hostidx={},connidx={}",hostidx,connidx);

        ChannelFuture future = null;
        if( startPort == -1 ) {
            future = bootstrap.connect(new InetSocketAddress(host,port));
        } else {
            future = bootstrap.connect(new InetSocketAddress(host,port),new InetSocketAddress(portIdx.getAndIncrement()));
            if( portIdx.get() >= 65535 ) portIdx.set(1);
        }

        future.addListener( new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) {
                onConnectCompleted(future,hostidx,connidx);
            }
        } );

        if( waitForAllConnected ) {

            futureLock.lock();
            try {
                int idx = hostidx + connidx * addrs.length;
                futures[idx] = future;
                futuresStartTime[idx] = System.currentTimeMillis();
                }	finally {
                    futureLock.unlock();
                }

        }

    }

    int connectedCount() {

        lock.lock();

        try {

            int i = 0;
            int cnt = 0;
            while(  i < channels.length ) {
                Channel ch = channels[i];
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

    void onConnectCompleted(ChannelFuture f,final int hostidx,final int connidx) {

        if (f.isCancelled()) {

            log.error("connect cancelled, hostidx="+hostidx+",connidx="+connidx);

            if( timer != null ) { // while shutdowning
                timer.newTimeout( new TimerTask() {

                    public void run( Timeout timeout) {
                        reconnect(hostidx,connidx);
                    }

                }, reconnectInterval, TimeUnit.SECONDS);
            }

        } else if (!f.isSuccess()) {

            log.error("connect failed, hostidx="+hostidx+",connidx="+connidx+",e="+f.getCause().getMessage());

            if( timer != null ) { // while shutdowning
                timer.newTimeout( new TimerTask() {

                    public void run(Timeout timeout) {
                        reconnect(hostidx,connidx);
                    }

                }, reconnectInterval, TimeUnit.SECONDS);
            }
        } else {

            Channel ch = f.getChannel();
            log.info("connect ok, hostidx="+hostidx+",connidx="+connidx+",channelId="+ch.getId()+",channelAddr="+addrs[hostidx]+",clientAddr="+ch.getLocalAddress().toString());
            int idx = hostidx + connidx * addrs.length;

            lock.lock();

            try {
                if( channels[idx] == null ) {
                    String theConnId = parseIpPort(ch.getRemoteAddress().toString()) + ":" + ch.getId();
                    channels[idx] = ch;
                    channelIds[idx] = theConnId;
                }
            } finally {
                lock.unlock();
            }

            if( waitForAllConnected ) {

                if( connectedCount() == channels.length ) {
                    connected.set(true);
                }

            } else {
                connected.set(true);
            }

            String ladr = ch.getLocalAddress().toString();
            if( NettyClient.localAddrsMap.contains(ladr) ) {
                log.warn("client addr duplicated, " + ladr );
            }	else {
                NettyClient.localAddrsMap.put(ladr,"1");
            }

            if( isSps ) {
                ByteBuffer buff = soc.generateReportSpsId();
                if( buff != null ) {
                    updateSpsId(buff,idx);
                    ChannelBuffer reqBuf = ChannelBuffers.wrappedBuffer(buff);
                    ch.write(reqBuf);
                }
            }
        }

    }
 
    String selectChannel() {

        lock.lock();

        try {
            int i = 0;

            while( i < channels.length ) {

                Channel channel = channels[i];
                String connId = channelIds[i];
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
    	Channel ch;
    	int idx;
    	ChannelAndIdx(Channel ch,int idx) {
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
            while(  i < channels.length ) {
                Channel ch = channels[nextIdx];
                String connId = channelIds[nextIdx];
                String chAddr = channelAddrs[nextIdx];
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

                            ChannelAndIdx d = new ChannelAndIdx(ch,nextIdx);
                            nextIdx += 1;
                            if( nextIdx >= channels.length ) nextIdx = 0;
                            return d;

                        }

                        } else {
                            log.error("channel not opened, idx={}, connId={}",i,connId);
                            removeChannel(connId);
                        }

                }
                i+=1;
                nextIdx += 1;
                if( nextIdx >= channels.length ) nextIdx = 0;
            }

            return new ChannelAndIdx(null,0);

        } finally {
            lock.unlock();
        }
    }
  
    void removeChannel(String connId) {

        if( shutdown.get()) {
            return;
        }

        lock.lock();

        int idx = -1;
        try {
        	int i = 0;

            while( idx == -1 && i < channels.length ) {
                Channel channel = channels[i];
                String theConnId = channelIds[i];
                if( channel != null && theConnId.equals(connId) ) {
                    channels[i] = null;
                    channelIds[i] = null;
                    idx = i;
                }

                i+=1;
            }

            channelsMap.remove(connId);

        } finally {
            lock.unlock();
        }

        if( idx != -1 ) {

            final int hostidx = idx % addrs.length;
            final int connidx = idx / addrs.length;

            timer.newTimeout( new TimerTask() {

                public void run( Timeout timeout) {
                    reconnect(hostidx,connidx);
                }

            }, reconnectInterval, TimeUnit.SECONDS);
        }

    }
    
 
    boolean send(int sequence,ByteBuffer buff,int timeout) {

    	ChannelAndIdx ci = nextChannel(sequence,timeout);

        if( ci.ch == null ) {
            return false;
        }

        if( isSps ) updateSpsId(buff,ci.idx);

        ChannelBuffer reqBuf = ChannelBuffers.wrappedBuffer(buff);
        ci.ch.write(reqBuf);

        return true;
    }


    boolean sendByAddr(int sequence,ByteBuffer buff ,int timeout,String addr) {

    	ChannelAndIdx ci = nextChannel(sequence,timeout,addr);

        if( ci.ch == null ) {
            return false;
        }

        if( isSps ) updateSpsId(buff,ci.idx);
        ChannelBuffer reqBuf = ChannelBuffers.wrappedBuffer(buff);
        ci.ch.write(reqBuf);

        return true;
    }

    void updateSpsId(ByteBuffer buff,int idx) {
        byte[] array = buff.array();
        //int hostidx = idx % addrs.length;
        int connidx = idx / addrs.length;
        String spsId = guids[connidx];
        byte[] spsIdArray = spsId.getBytes(); // "ISO-8859-1"
        int i = 0;
        while( i < spsIdArray.length ) {
            array[44+4+i] = spsIdArray[i] ;// start from the xhead (44), skip the xhead spsId head (4)
            i += 1;
        }
    }

    boolean sendByConnId(int sequence, ByteBuffer buff ,int timeout,String connId) {

        Channel ch = nextChannelFromMap(sequence,timeout,connId);

        if( ch == null ) {
            return false;
        }

        ChannelBuffer reqBuf = ChannelBuffers.wrappedBuffer(buff);
        ch.write(reqBuf);

        return true;
    }

    boolean sendResponse(int sequence,ByteBuffer buff,String connId) {

    	Channel ch = null;

        lock.lock();

        try {
            int i = 0;

            while( i < channels.length && ch == null ) {
                Channel channel = channels[i];
                String theConnId = channelIds[i];
                if( channel != null && theConnId.equals(connId) ) {
                    ch = channels[i];
                }

                i+=1;
            }
        } finally {
            lock.unlock();
        }

        if( ch == null ) {
            return false;
        }

        ChannelBuffer reqBuf = ChannelBuffers.wrappedBuffer(buff);
        ch.write(reqBuf);

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

    void onReceive(ByteBuffer buff,String connId) {

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
        ChannelBuffer buf = (ChannelBuffer)e.getMessage();
        ByteBuffer bb = buf.toByteBuffer();
        onReceive(bb,connId);
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
        ByteBuffer buff = soc.generatePing();
        ChannelBuffer reqBuf = ChannelBuffers.wrappedBuffer(buff);
        ch.write(reqBuf);
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

}

