package org.apache.ignite.internal.util.nio.channel;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketOptions;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import org.apache.ignite.IgniteException;

/**
 * A channel configuration for the {@link GridNioSocketChannel}.
 */
public final class GridNioSocketChannelConfig {
    /** */
    private final SocketChannel channel;

    /** */
    private final Socket socket;

    /**
     *
     */
    public GridNioSocketChannelConfig(SocketChannel channel) {
        this.channel = channel;
        this.socket = channel.socket();
    }

    /**
     * Gets the {@link AbstractSelectableChannel#isBlocking()} mode.
     */
    public boolean isBlocking() {
        return channel.isBlocking();
    }

    /**
     * Sets channel's blocking mode by {@link AbstractSelectableChannel#configureBlocking(boolean)} .
     */
    public GridNioSocketChannelConfig setBlocking(boolean blocking) {
        try {
            channel.configureBlocking(blocking);
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }

        return this;
    }

    /**
     * Gets the {@link SocketOptions#TCP_NODELAY} option.
     */
    public boolean isTcpNoDelay() {
        try {
            return socket.getTcpNoDelay();
        }
        catch (SocketException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Sets the {@link SocketOptions#TCP_NODELAY} option.
     */
    public GridNioSocketChannelConfig setTcpNoDelay(boolean tcpNoDelay) {
        try {
            socket.setTcpNoDelay(tcpNoDelay);
        }
        catch (SocketException e) {
            throw new IgniteException(e);
        }

        return this;
    }

    /**
     * Gets the {@link SocketOptions#SO_SNDBUF} option.
     */
    public int getSendBufferSize() {
        try {
            return socket.getSendBufferSize();
        }
        catch (SocketException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Sets the {@link SocketOptions#SO_SNDBUF} option.
     */
    public GridNioSocketChannelConfig setSendBufferSize(int sendBufferSize) {
        try {
            socket.setSendBufferSize(sendBufferSize);
        }
        catch (SocketException e) {
            throw new IgniteException(e);
        }

        return this;
    }

    /**
     * Gets the {@link SocketOptions#SO_RCVBUF} option.
     */
    public int getReceiveBufferSize() {
        try {
            return socket.getReceiveBufferSize();
        }
        catch (SocketException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Sets the {@link SocketOptions#SO_RCVBUF} option.
     */
    public GridNioSocketChannelConfig setReceiveBufferSize(int receiveBufferSize) {
        try {
            socket.setReceiveBufferSize(receiveBufferSize);
        }
        catch (SocketException e) {
            throw new IgniteException(e);
        }

        return this;
    }

    /**
     * Gets the {@link SocketOptions#SO_TIMEOUT} option.
     */
    public int getConnectTimeoutMillis() {
        try {
            return socket.getSoTimeout();
        }
        catch (SocketException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Sets the {@link SocketOptions#SO_TIMEOUT} option.
     */
    public GridNioSocketChannelConfig setConnectTimeoutMillis(int connectTimeoutMillis) {
        try {
            socket.setSoTimeout(connectTimeoutMillis);
        }
        catch (SocketException e) {
            throw new IgniteException(e);
        }

        return this;
    }
}
