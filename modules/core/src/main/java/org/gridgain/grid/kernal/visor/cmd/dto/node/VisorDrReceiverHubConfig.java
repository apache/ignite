/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import org.gridgain.grid.dr.hub.receiver.*;

import java.io.*;

/**
 * Visor counterpart for {@link GridDrReceiverHubConfiguration}.
 */
public class VisorDrReceiverHubConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Local host name receiving hub server is bound to. */
    private final String localInboundHost;

    /** Local port receiving hub is bound to. */
    private final int localInboundPort;

    /** Number of selector threads in receiver hub's TCP server. */
    private final int selectorCount;

    /** Number of threads responsible for sender hub requests processing. */
    private final int workerThreads;

    /** Message queue limit for incoming and outgoing messages. */
    private final int messageQueueLimit;

    /** Whether to use TCP_NODELAY mode. */
    private final boolean tcpNodelay;

    /** Whether to use direct buffer when processing sender hub requests. */
    private final boolean directBuffer;

    /** Idle timeout for sender hub socket connection. */
    private final long idleTimeout;

    /** Write timeout for sender hub socket connection. */
    private final long writeTimeout;

    /** DR data loader flush frequency. */
    private final long flushFrequency;

    /** DR data loader per node buffer size. */
    private final int perNodeBufferSize;

    /** Per node parallel load operations. */
    private final int perNodeParallelLoadOperations;

    public VisorDrReceiverHubConfig(String localInboundHost, int localInboundPort, int selectorCount, int workerThreads,
        int messageQueueLimit, boolean tcpNodelay, boolean directBuffer, long idleTimeout, long writeTimeout,
        long flushFrequency, int perNodeBufferSize, int perNodeParallelLoadOperations) {
        this.localInboundHost = localInboundHost;
        this.localInboundPort = localInboundPort;
        this.selectorCount = selectorCount;
        this.workerThreads = workerThreads;
        this.messageQueueLimit = messageQueueLimit;
        this.tcpNodelay = tcpNodelay;
        this.directBuffer = directBuffer;
        this.idleTimeout = idleTimeout;
        this.writeTimeout = writeTimeout;
        this.flushFrequency = flushFrequency;
        this.perNodeBufferSize = perNodeBufferSize;
        this.perNodeParallelLoadOperations = perNodeParallelLoadOperations;
    }

    /**
     * @return Local host name receiving hub server is bound to.
     */
    public String localInboundHost() {
        return localInboundHost;
    }

    /**
     * @return Local port receiving hub is bound to.
     */
    public int localInboundPort() {
        return localInboundPort;
    }

    /**
     * @return Number of selector threads in receiver hub's TCP server.
     */
    public int selectorCount() {
        return selectorCount;
    }

    /**
     * @return Number of threads responsible for sender hub requests processing.
     */
    public int workerThreads() {
        return workerThreads;
    }

    /**
     * @return Message queue limit for incoming and outgoing messages.
     */
    public int messageQueueLimit() {
        return messageQueueLimit;
    }

    /**
     * @return Whether to use TCP_NODELAY mode.
     */
    public boolean tcpNodelay() {
        return tcpNodelay;
    }

    /**
     * @return Whether to use direct buffer when processing sender hub requests.
     */
    public boolean directBuffer() {
        return directBuffer;
    }

    /**
     * @return Idle timeout for sender hub socket connection.
     */
    public long idleTimeout() {
        return idleTimeout;
    }

    /**
     * @return Write timeout for sender hub socket connection.
     */
    public long writeTimeout() {
        return writeTimeout;
    }

    /**
     * @return DR data loader flush frequency.
     */
    public long flushFrequency() {
        return flushFrequency;
    }

    /**
     * @return DR data loader per node buffer size.
     */
    public int perNodeBufferSize() {
        return perNodeBufferSize;
    }

    /**
     * @return Per node parallel load operations.
     */
    public int perNodeParallelLoadOperations() {
        return perNodeParallelLoadOperations;
    }
}
