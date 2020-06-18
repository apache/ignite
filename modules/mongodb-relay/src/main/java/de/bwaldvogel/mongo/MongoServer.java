package de.bwaldvogel.mongo;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.wire.MongoDatabaseHandler;
import de.bwaldvogel.mongo.wire.MongoExceptionHandler;
import de.bwaldvogel.mongo.wire.MongoWireEncoder;
import de.bwaldvogel.mongo.wire.MongoWireProtocolHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

public class MongoServer {

    private static final Logger log = LoggerFactory.getLogger(MongoServer.class);

    private final MongoBackend backend;

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private ChannelGroup channelGroup;

    private Channel channel;

    private SslContext sslContext;

    public MongoServer(MongoBackend backend) {
        this.backend = backend;
    }

    public void enableSsl(PrivateKey key, String keyPassword, X509Certificate... keyCertChain) {
        Assert.isNull(channel, () -> "Server already started");
        try {
            sslContext = SslContextBuilder.forServer(key, keyPassword, keyCertChain).build();
        } catch (SSLException e) {
            throw new RuntimeException("Failed to enable SSL", e);
        }
    }

    public void bind(String hostname, int port) {
        bind(new InetSocketAddress(hostname, port));
    }

    public void bind(SocketAddress socketAddress) {

        bossGroup = new NioEventLoopGroup(0, new MongoThreadFactory("mongo-server-boss"));
        workerGroup = new NioEventLoopGroup(0, new MongoThreadFactory("mongo-server-worker"));
        channelGroup = new DefaultChannelGroup("mongodb-channels", workerGroup.next());

        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 100)
                .localAddress(socketAddress)
                .childOption(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        if (sslContext != null) {
                            ch.pipeline().addLast(sslContext.newHandler(ch.alloc()));
                        }
                        ch.pipeline().addLast(new MongoWireEncoder());
                        ch.pipeline().addLast(new MongoWireProtocolHandler());
                        ch.pipeline().addLast(new MongoDatabaseHandler(backend, channelGroup));
                        ch.pipeline().addLast(new MongoExceptionHandler());
                    }
                });

            channel = bootstrap.bind().syncUninterruptibly().channel();

            log.info("started {}", this);
        } catch (RuntimeException e) {
            shutdownNow();
            throw e;
        }
    }

    /**
     * starts and binds the server on a local random port
     *
     * @return the random local address the server was bound to
     */
    public InetSocketAddress bind() {
        bind(new InetSocketAddress("localhost", 0));
        return getLocalAddress();
    }

    /**
     * @return the local address the server was bound or null if the server is
     *         not listening
     */
    public InetSocketAddress getLocalAddress() {
        if (channel == null) {
            return null;
        }
        return (InetSocketAddress) channel.localAddress();
    }

    /**
     * Stop accepting new clients. Wait until all resources (such as client
     * connection) are closed and then shutdown. This method blocks until all
     * clients are finished. Use {@link #shutdownNow()} if the shutdown should
     * be forced.
     */
    public void shutdown() {
        stopListenting();

        // Shut down all event loops to terminate all threads.
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }

        if (bossGroup != null) {
            bossGroup.terminationFuture().syncUninterruptibly();
        }
        if (workerGroup != null) {
            workerGroup.terminationFuture().syncUninterruptibly();
        }

        backend.close();

        log.info("completed shutdown of {}", this);
    }

    /**
     * Closes the server socket. No new clients are accepted afterwards.
     */
    public void stopListenting() {
        if (channel != null) {
            log.info("closing server channel");
            channel.close().syncUninterruptibly();
            channel = null;
        }
    }

    /**
     * Stops accepting new clients, closes all clients and finally shuts down
     * the server In contrast to {@link #shutdown()}, this method should not
     * block.
     */
    public void shutdownNow() {
        stopListenting();
        closeClients();
        shutdown();
    }

    private void closeClients() {
        if (channelGroup != null) {
            int numClients = channelGroup.size();
            if (numClients > 0) {
                log.warn("Closing {} clients", numClients);
            }
            channelGroup.close().syncUninterruptibly();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        sb.append("(");
        InetSocketAddress socketAddress = getLocalAddress();
        if (socketAddress != null) {
            sb.append("port: ").append(socketAddress.getPort());
            sb.append(", ssl: ").append(sslContext != null);
        }
        sb.append(")");
        return sb.toString();
    }
}
