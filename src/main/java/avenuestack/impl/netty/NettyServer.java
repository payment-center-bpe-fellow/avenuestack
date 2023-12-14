package avenuestack.impl.netty;

import avenuestack.impl.util.NamedThreadFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class NettyServerHandler extends IdleStateAwareChannelHandler {

	static Logger log = LoggerFactory.getLogger(NettyServerHandler.class);
	
	NettyServer nettyServer;
	Sos4Netty sos;
	
	ConcurrentHashMap<String,Channel> conns = new ConcurrentHashMap<String,Channel>();
	AtomicInteger new_conns = new AtomicInteger(0);
	AtomicInteger new_disconns = new AtomicInteger(0);

	AtomicBoolean stopFlag = new AtomicBoolean();

	public NettyServerHandler(NettyServer nettyServer,Sos4Netty sos) {
		this.nettyServer = nettyServer;
		this.sos = sos;
	}
	
    void close() {
    	stopFlag.set(true);
    }

    int[] stats() {
        int a = new_conns.getAndSet(0);
        int b = new_disconns.getAndSet(0);
        return new int[] { a,b,conns.size() } ;
    }

    boolean write(String connId,ChannelBuffer response) {

        if( response == null ) return false;

        Channel ch = conns.get(connId);
        if( ch == null ) {
            log.error("connection not found, id={}",connId);
            return false;
        }

        if( ch.isOpen() ) {
            ch.write(response);
            return true;
        }

        return false;
    }

    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {

    	Channel ch = e.getChannel();
        if (stopFlag.get()) {
            ch.setReadable(false);
            return;
        }

        ChannelBuffer buf = (ChannelBuffer)e.getMessage();
        String connId = (String)ctx.getAttachment();
        try {
            sos.receive(buf, connId);
        } catch(Exception ex) {
            log.error("sos decode error, connId="+connId,ex);
        }
    }

    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {

        new_conns.incrementAndGet();

        Channel ch = e.getChannel();
        String connId = parseIpPort(ch.getRemoteAddress().toString()) + ":" + ch.getId();
        ctx.setAttachment(connId);

        if( conns.size() >= nettyServer.maxConns ) {
            log.error("connection started, id={}, but max connections exceeded, conn not allowed",connId);
            ch.close();
            return;
        }

        log.info("connection started, id={}",connId);

        conns.put(connId,ch);
        sos.connected(connId);

        if (stopFlag.get()) {
            ch.close();
        }
    }

    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) {

        new_disconns.incrementAndGet();

        String connId = (String)ctx.getAttachment();
        log.info("connection ended, id={}",connId);
        conns.remove(connId);
        sos.disconnected(connId);
    }

    public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent e) {
    	String connId = (String)ctx.getAttachment();
        log.error("connection timeout, id={}",connId);
        e.getChannel().close();
    }

    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    	String connId = (String)ctx.getAttachment();
        log.error("connection exception, id={},msg={}",connId,e.toString());
        e.getChannel().close();
    }

    String parseIpPort(String s) {
        int p = s.indexOf("/");

        if (p >= 0)
            return s.substring(p + 1);
        else
        	return s;
    }

}

class NettyServer { // with Dumpable 

	static Logger log = LoggerFactory.getLogger(NettyServer.class);
	
	Sos4Netty sos;
    int port;
    String host = "*";
    int idleTimeoutMillis = 180000;
    int maxPackageSize = 2000000;
    int maxConns = 500000;
    int[] ports;
    
	NettyServerHandler nettyServerHandler;
	ChannelGroup allChannels;
	ChannelFactory channelFactory;
	Timer timer;

	NamedThreadFactory bossThreadFactory = new NamedThreadFactory("sosboss");
	NamedThreadFactory workThreadFactory = new NamedThreadFactory("soswork");
	NamedThreadFactory timerThreadFactory = new NamedThreadFactory("sostimer");

	ThreadPoolExecutor bossExecutor;
	ThreadPoolExecutor workerExecutor;

	public NettyServer(Sos4Netty sos,
		    int port, String host, int idleTimeoutMillis ,
		    int maxPackageSize, int maxConns) {

		this.sos = sos;
		this.port = port;
		this.host = host;
		this.idleTimeoutMillis = idleTimeoutMillis;
		this.maxPackageSize = maxPackageSize;
		this.maxConns = maxConns;
	}

    public NettyServer(Sos4Netty sos,int port,
                       int[] ports, String host, int idleTimeoutMillis ,
                       int maxPackageSize, int maxConns) {

        this.sos = sos;
        if(ports==null || ports.length == 0) {
            this.ports = new int[1];
            this.ports[0] = port;
        } else {
            this.ports = ports;
        }
        this.host = host;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.maxPackageSize = maxPackageSize;
        this.maxConns = maxConns;
    }
	
    int[] stats() {
        return nettyServerHandler.stats();
    }

    void dump() {
        if( nettyServerHandler == null ) return;

        StringBuilder buff = new StringBuilder();

        buff.append("nettyServerHandler.conns.size=").append(nettyServerHandler.conns.size()).append(",");
        buff.append("bossExecutor.getPoolSize=").append(bossExecutor.getPoolSize()).append(",");
        buff.append("bossExecutor.getQueue.size=").append(bossExecutor.getQueue().size()).append(",");
        buff.append("workerExecutor.getPoolSize=").append(workerExecutor.getPoolSize()).append(",");
        buff.append("workerExecutor.getQueue.size=").append(workerExecutor.getQueue().size()).append(",");

        log.info(buff.toString());
    }

    void start() {

        nettyServerHandler = new NettyServerHandler(this,sos);

        // without this line, the thread name of netty will not be changed
        ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT); // or PROPOSED

        timer = new HashedWheelTimer(timerThreadFactory,1,TimeUnit.SECONDS);

        allChannels = new DefaultChannelGroup("netty-server-scala");

        bossExecutor = (ThreadPoolExecutor)Executors.newCachedThreadPool(bossThreadFactory);
        workerExecutor = (ThreadPoolExecutor)Executors.newCachedThreadPool(workThreadFactory);
        channelFactory = new NioServerSocketChannelFactory(bossExecutor ,workerExecutor );

        ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
        bootstrap.setPipelineFactory(new PipelineFactory());
        bootstrap.setOption("child.tcpNoDelay", true);
        // bootstrap.setOption("child.keepAlive", true)
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("child.receiveBufferSize", 65536);

        InetSocketAddress addr;

        for (int port : ports) {
            if (host == null || "*".equals(host) ) {
                addr = new InetSocketAddress(port);
            } else {
                addr = new InetSocketAddress(host, port);
            }

            Channel channel = bootstrap.bind(addr);
            allChannels.add(channel);
        }

        String s = "netty tcp server started on host(" + host + ") port(" + Arrays.toString(ports) + ")";
        log.info(s);
    }

    boolean write(String connId, ChannelBuffer response){
        return nettyServerHandler.write(connId,response);
    }

    void closeReadChannel() {
        if( nettyServerHandler != null ) {
            nettyServerHandler.close();
        }
        log.info("nettyServerHandler read channel stopped");
    }

    void close() {

        if (channelFactory != null) {

            log.info("Stopping NettyServer");

            timer.stop();
            timer = null;

            for(Channel ch: nettyServerHandler.conns.values()) {
            	allChannels.add(ch);
            }
            ChannelGroupFuture future = allChannels.close();
            future.awaitUninterruptibly();
            allChannels = null;

            channelFactory.releaseExternalResources();
            channelFactory = null;

            log.info("netty server stopped");
        }
    }

    class PipelineFactory implements ChannelPipelineFactory {

    	public ChannelPipeline getPipeline() {
    		ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("timeout", new IdleStateHandler(timer, 0, 0, idleTimeoutMillis / 1000));
            LengthFieldBasedFrameDecoder decoder = new LengthFieldBasedFrameDecoder(maxPackageSize, 4, 4, -8, 0);
            pipeline.addLast("decoder", decoder);
            pipeline.addLast("handler", nettyServerHandler);
            return pipeline;
        }
    }

}


