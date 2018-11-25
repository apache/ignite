package org.apache.ignite.internal.util.nio.channel;

import java.net.SocketOptions;
import java.nio.channels.SocketChannel;

/**
 * A channel configuration for the {@link SocketChannel}.
 *
 * <h3>Available options</h3>
 *
 * The {@link GridNioSocketChannelConfig} allows the following options in the option map:
 *
 * <table border="1" cellspacing="0" cellpadding="6">
 * <tr>
 * <th>Name</th><th>Associated setter method</th>
 * </tr><tr>
 * <td>{@link SocketOptions#SO_KEEPALIVE}</td><td>{@link #setKeepAlive(boolean)}</td>
 * </tr><tr>
 * <td>{@link SocketOptions#TCP_NODELAY}</td><td>{@link #setTcpNoDelay(boolean)}</td>
 * </tr><tr>
 * <td>{@link SocketOptions#SO_RCVBUF}</td><td>{@link #setReceiveBufferSize(int)}</td>
 * </tr><tr>
 * <td>{@link SocketOptions#SO_SNDBUF}</td><td>{@link #setSendBufferSize(int)}</td>
 * </tr><tr>
 * <td>{@link SocketOptions#SO_TIMEOUT}</td><td>{@link #setConnectTimeoutMillis(int)}</td>
 * </tr>
 * </table>
 */
public interface GridNioSocketChannelConfig {
    /**
     * Gets the {@link SocketOptions#TCP_NODELAY} option.
     */
    public boolean isTcpNoDelay();

    /**
     * Sets the {@link SocketOptions#TCP_NODELAY} option.
     */
    public GridNioSocketChannelConfig setTcpNoDelay(boolean tcpNoDelay);

    /**
     * Gets the {@link SocketOptions#SO_SNDBUF} option.
     */
    public int getSendBufferSize();

    /**
     * Sets the {@link SocketOptions#SO_SNDBUF} option.
     */
    public GridNioSocketChannelConfig setSendBufferSize(int sendBufferSize);

    /**
     * Gets the {@link SocketOptions#SO_RCVBUF} option.
     */
    public int getReceiveBufferSize();

    /**
     * Sets the {@link SocketOptions#SO_RCVBUF} option.
     */
    public GridNioSocketChannelConfig setReceiveBufferSize(int receiveBufferSize);

    /**
     * Gets the {@link SocketOptions#SO_KEEPALIVE} option.
     */
    public boolean isKeepAlive();

    /**
     * Sets the {@link SocketOptions#SO_KEEPALIVE} option.
     */
    public GridNioSocketChannelConfig setKeepAlive(boolean keepAlive);

    /**
     * Gets the {@link SocketOptions#SO_TIMEOUT} option.
     */
    public int getConnectTimeoutMillis();

    /**
     * Sets the {@link SocketOptions#SO_TIMEOUT} option.
     */
    public GridNioSocketChannelConfig setConnectTimeoutMillis(int connectTimeoutMillis);

}
