package avenuestack.impl.netty;

import java.net.InetSocketAddress;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.jboss.netty.bootstrap.ClientBootstrap;
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
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestEncoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseDecoder;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import avenuestack.impl.util.NamedThreadFactory;
import avenuestack.impl.util.QuickTimer;
import avenuestack.impl.util.QuickTimerEngine;
import avenuestack.impl.util.QuickTimerFunction;

public class NettyHttpClient { // with Dumpable

	static Logger log = LoggerFactory.getLogger(NettyHttpClient.class);
	static AtomicInteger count = new AtomicInteger(1);

	HttpClient4Netty httpClient;
    int connectTimeout;
	int timerInterval;
	int maxContentLength;

	NioClientSocketChannelFactory factory;
	NettyPipelineFactory pipelineFactory;
	SslNettyPipelineFactory sslPipelineFactory;
	NettyHttpClientHandler nettyHttpClientHandler;

    QuickTimerFunction onTimeoutFunction = new QuickTimerFunction() {
    	public void call(Object a) {
    		onTimeout(a);
    	}
    };
    
	QuickTimerEngine qte = new QuickTimerEngine(onTimeoutFunction,timerInterval);

	ConcurrentHashMap dataMap = new ConcurrentHashMap<Integer,TimeoutInfo>(); // key is sequence
	ConcurrentHashMap connMap = new ConcurrentHashMap<Integer,TimeoutInfo>(); // key is channel.getId

    NamedThreadFactory bossThreadFactory = new NamedThreadFactory("httpclientboss"+NettyHttpClient.count.getAndIncrement());
    NamedThreadFactory workThreadFactory = new NamedThreadFactory("httpclientwork"+NettyHttpClient.count.getAndIncrement());

    ThreadPoolExecutor bossExecutor;
    ThreadPoolExecutor workerExecutor;


    public NettyHttpClient(
		HttpClient4Netty httpClient,
	    int connectTimeout,
		int timerInterval,
		int maxContentLength ) {
    	
    	this.httpClient = httpClient;
    	this.connectTimeout = connectTimeout;
    	this.timerInterval = timerInterval;
    	this.maxContentLength = maxContentLength;

    	pipelineFactory = new NettyPipelineFactory();

    	init();
    }

    public void dump() {

    	StringBuilder buff = new StringBuilder();

        buff.append("dataMap.size=").append(dataMap.size()).append(",");
        buff.append("connMap.size=").append(connMap.size()).append(",");

        buff.append("bossExecutor.getPoolSize=").append(bossExecutor.getPoolSize()).append(",");
        buff.append("bossExecutor.getQueue.size=").append(bossExecutor.getQueue().size()).append(",");
        buff.append("workerExecutor.getPoolSize=").append(workerExecutor.getPoolSize()).append(",");
        buff.append("workerExecutor.getQueue.size=").append(workerExecutor.getQueue().size()).append(",");

        log.info(buff.toString());

        qte.dump();
    }

    public void close() {

        if (factory != null) {

            // TODO close existed channels

            log.info("stopping netty http client");
            factory.releaseExternalResources();
            factory = null;
        }

        qte.close();

        log.info("netty http client stopped");
    }

    void init() {

        nettyHttpClientHandler = new NettyHttpClientHandler(this);

        // without this line, the thread name of netty will not be changed
        ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT); // or PROPOSED

        bossExecutor = (ThreadPoolExecutor)Executors.newCachedThreadPool(bossThreadFactory);
        workerExecutor = (ThreadPoolExecutor)Executors.newCachedThreadPool(workThreadFactory);

        factory = new NioClientSocketChannelFactory(bossExecutor, workerExecutor);

        log.info("netty http client started");
    }

    public void send(final int sequence,boolean ssl,final String addr,final HttpRequest httpReq,final int timeout) {

        String[] ss = addr.split(":");
        String host = ss[0];
        int port = ss.length >= 2 ? Integer.parseInt(ss[1]) : 80;

        ClientBootstrap bootstrap = new ClientBootstrap(factory);
        if( ssl ) {

            if( SslContextFactory.CLIENT_CONTEXT == null ) {
                httpClient.networkError(sequence);
                return;
            }
            if( sslPipelineFactory == null )
                sslPipelineFactory = new SslNettyPipelineFactory();
            bootstrap.setPipelineFactory(sslPipelineFactory);

        } else {

            bootstrap.setPipelineFactory(pipelineFactory);

        }
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);

        int t = timeout < connectTimeout ? timeout : connectTimeout;
        bootstrap.setOption("connectTimeoutMillis", t);

        final ChannelFuture future = bootstrap.connect(new InetSocketAddress(host,port));
        future.addListener( new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) {
                onConnectCompleted(future,sequence,addr,httpReq,timeout);
            }
        } );

    }

    void onConnectCompleted(ChannelFuture f, int sequence,String addr,HttpRequest httpReq,int timeout)  {

        if (f.isCancelled()) {
            log.error("onConnectCompleted f.isCancelled should not be called");
        } else if (!f.isSuccess()) {
            log.error("connect failed, addr={},e={}",addr,f.getCause().getMessage());
            httpClient.networkError(sequence);
        } else {
            // log.debug(addr+" connected");
            Channel ch = f.getChannel();

            QuickTimer t = qte.newTimer(timeout,sequence);
            TimeoutInfo ti = new TimeoutInfo(sequence,ch,t);
            dataMap.put(sequence,ti);
            connMap.put(ch.getId(),ti);

            ch.write(httpReq);
        }

    }

    void onTimeout(Object data) {

        int sequence = (Integer)data;

        TimeoutInfo ti = (TimeoutInfo)dataMap.remove(sequence);
        if( ti != null ) {
            connMap.remove(ti.channel.getId());
            httpClient.timeoutError(sequence);
            ti.channel.close();
        } else {
            log.error("timeout but sequence not found, seq={}",sequence);
        }

    }

    void messageReceived(ChannelHandlerContext ctx,MessageEvent e) {
        Channel ch = e.getChannel();

        TimeoutInfo ti = (TimeoutInfo)connMap.remove(ch.getId());
        if( ti == null ) return;
        dataMap.remove(ti.sequence);
        ti.timer.cancel();

        HttpResponse httpRes = (HttpResponse)e.getMessage();

        httpClient.receive(ti.sequence,httpRes);

        ch.close();
    }

    void exceptionCaught(ChannelHandlerContext ctx,ExceptionEvent e)  {
    	Channel ch = e.getChannel();

    	TimeoutInfo ti = (TimeoutInfo)connMap.remove(ch.getId());
        if( ti == null ) return;
        dataMap.remove(ti.sequence);
        ti.timer.cancel();

        String remoteAddr = ch.getRemoteAddress().toString();
        log.error("exceptionCaught addr={},e={}",remoteAddr,e);

        httpClient.networkError(ti.sequence);

        ch.close();
    }

    void channelDisconnected(ChannelHandlerContext ctx,ChannelStateEvent e)  {
    	Channel ch = e.getChannel();
        String remoteAddr = ch.getRemoteAddress().toString();
        // log.debug(remoteAddr+" disconnected");

        // remove dataMap

        TimeoutInfo ti = (TimeoutInfo)connMap.remove(ch.getId());
        if( ti == null ) return;
        dataMap.remove(ti.sequence);
        ti.timer.cancel();

    }

    class NettyPipelineFactory implements ChannelPipelineFactory {

    	public ChannelPipeline getPipeline() {
    		ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("decoder", new HttpResponseDecoder());
            pipeline.addLast("aggregator", new HttpChunkAggregator(maxContentLength));
            pipeline.addLast("encoder", new HttpRequestEncoder());
            pipeline.addLast("handler", nettyHttpClientHandler);
            return pipeline;
        }
    }

    class SslNettyPipelineFactory implements ChannelPipelineFactory {

    	public ChannelPipeline getPipeline() {
    		ChannelPipeline pipeline = Channels.pipeline();

    		SSLEngine engine = SslContextFactory.CLIENT_CONTEXT.createSSLEngine();
            engine.setUseClientMode(true);

            pipeline.addLast("ssl", new SslHandler(engine));

            pipeline.addLast("decoder", new HttpResponseDecoder());
            pipeline.addLast("aggregator", new HttpChunkAggregator(maxContentLength));
            pipeline.addLast("encoder", new HttpRequestEncoder());
            pipeline.addLast("handler", nettyHttpClientHandler);
            return pipeline;
        }
    }

    class NettyHttpClientHandler extends SimpleChannelUpstreamHandler {

    	NettyHttpClient nettyHttpClient;
    	
    	NettyHttpClientHandler(NettyHttpClient nettyHttpClient) {
    		this.nettyHttpClient = nettyHttpClient;
    	}
    	
        public void messageReceived(ChannelHandlerContext ctx,MessageEvent e) {
            nettyHttpClient.messageReceived(ctx,e);
        }

        public void exceptionCaught(ChannelHandlerContext ctx,ExceptionEvent e) {
            nettyHttpClient.exceptionCaught(ctx,e);
        }

        public void channelDisconnected(ChannelHandlerContext ctx,ChannelStateEvent e) {
            nettyHttpClient.channelDisconnected(ctx,e);
        }

    }

    class TimeoutInfo {
    	int sequence;
    	Channel channel;
    	QuickTimer timer;
    	
    	TimeoutInfo(int sequence,Channel channel,QuickTimer timer) {
    		this.sequence = sequence;
    		this.channel = channel;
    		this.timer = timer;
    	}
    }

}

class SslContextFactory {

	static String PROTOCOL = "TLS";
    static SSLContext CLIENT_CONTEXT;

    static {
    	init();
    }

    static void init() {

        String algorithm = Security.getProperty("ssl.KeyManagerFactory.algorithm");
        if (algorithm == null) {
            algorithm = "SunX509";
        }

        try {
        	SSLContext clientContext = SSLContext.getInstance(PROTOCOL);
            clientContext.init(null, SslTrustManagerFactory.getTrustManagers(), null);
            CLIENT_CONTEXT = clientContext;
        } catch(Exception e) {
                throw new Error("Failed to initialize the client-side SSLContext", e);
        }

    }

}

class SslTrustManagerFactory {

	static Logger log = LoggerFactory.getLogger(SslTrustManagerFactory.class);
	
	static X509TrustManager DUMMY_TRUST_MANAGER = new X509TrustManager() {

        public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        public void checkClientTrusted(X509Certificate[] chain,String authType) {}

        /*

        ssl certificate from alipay:

        SERVER CERTIFICATE: CN=*.alipay.com, OU=Terms of use at www.verisign.com/rpa (c)05,
        OU=Operations Department, O="Alipay.com Co.,Ltd", L=HANGZHOU, ST=ZHEJIANG, C=CN

        ssl certificate from google:

        SERVER CERTIFICATE: CN=www.google.com,
        O=Google Inc, L=Mountain View, ST=California, C=US


         */

        public void checkServerTrusted(X509Certificate[] chain,String authType) {
            // TODO Always trust any server certificate, should do something.
            if( log.isDebugEnabled() ) {
                log.debug("SERVER CERTIFICATE: " + chain[0].getSubjectDN());
            }
        }
    };

    static TrustManager[] getTrustManagers() {
        return new TrustManager[] { DUMMY_TRUST_MANAGER };
    }

}

