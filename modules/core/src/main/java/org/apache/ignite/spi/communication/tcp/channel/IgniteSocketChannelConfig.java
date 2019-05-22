package org.apache.ignite.spi.communication.tcp.channel;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.SocketChannel;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.communication.ChannelConfig;

/**
 * A channel configuration for the {@link IgniteSocketChannel}.
 */
public final class IgniteSocketChannelConfig implements ChannelConfig {
    /** */
    private final SocketChannel channel;

    /** */
    private final Socket socket;

    /**
     * @param channel The socket channel to create configuration from.
     */
    public IgniteSocketChannelConfig(SocketChannel channel) {
        this.channel = channel;
        this.socket = channel.socket();
    }

    /** {@inheritDoc} */
    @Override public boolean blocking() {
        return channel.isBlocking();
    }

    /** {@inheritDoc} */
    @Override public ChannelConfig blocking(boolean blocking) {
        try {
            channel.configureBlocking(blocking);
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public int timeout() {
        try {
            return socket.getSoTimeout();
        }
        catch (SocketException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteSocketChannelConfig timeout(int millis) {
        try {
            socket.setSoTimeout(millis);
        }
        catch (SocketException e) {
            throw new IgniteException(e);
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteSocketChannelConfig.class, this);
    }
}
