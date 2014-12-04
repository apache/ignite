/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.hub.receiver;

import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Data center replication receiver hub configuration.
 */
public class GridDrReceiverHubConfiguration {
    /** Default server port. */
    public static final int DFLT_LOCAL_PORT = 49000;

    /** Default quantity of NIO threads responsible for sender hub connections processing.  */
    public static final int DFLT_SELECTOR_CNT = Math.min(4, Runtime.getRuntime().availableProcessors());

    /** Default number of threads responsible for sender hub requests processing.  */
    public static final int DFLT_WORKER_THREAD_CNT = Runtime.getRuntime().availableProcessors() * 4;

    /** Default message queue limit per connection (for incoming and outgoing . */
    public static final int DFLT_MSG_QUEUE_LIMIT = GridNioServer.DFLT_SEND_QUEUE_LIMIT;

    /** Default TCP_NODELAY flag value for server sockets. */
    public static final boolean DFLT_TCP_NODELAY = true;

    /** Default value of direct buffer allocation. */
    public static final boolean DFLT_DIRECT_BUF = true;

    /** Default server socket idle timeout. */
    public static final long DFLT_IDLE_TIMEOUT = 60 * 1000L;

    /** Default server socket write timeout. */
    public static final long DFLT_WRITE_TIMEOUT = 60 * 1000L;

    /** Default flush frequency DR data loader. */
    public static final long DFLT_FLUSH_FREQ = 2000L;

    /** Default per node buffer buffer size for DR data loader. */
    public static final int DFLT_PER_NODE_BUF_SIZE = 1024;

    /** Default per node parallel operations for DR data loader. */
    public static final int DFLT_PARALLEL_LOAD_OPS = 16;

    /** Replication server host. */
    private String locInboundHost;

    /** Replication server port. */
    private int locInboundPort = DFLT_LOCAL_PORT;

    /** Quantity of NIO threads responsible for sender hub connections processing. */
    private int selectorCnt = DFLT_SELECTOR_CNT;

    /**  Number of threads responsible for sender hub requests processing. */
    private int workerCnt = DFLT_WORKER_THREAD_CNT;

    /** Message queue limit. */
    private int msgQueueLimit = DFLT_MSG_QUEUE_LIMIT;

    /** TCP_NODELAY flag value for server sockets. */
    private boolean tcpNodelay = DFLT_TCP_NODELAY;

    /** Direct buffer allocation flag. */
    private boolean directBuf = DFLT_DIRECT_BUF;

    /** Server socket idle timeout. */
    private long idleTimeout = DFLT_IDLE_TIMEOUT;

    /** Server socket write timeout. */
    private long writeTimeout = DFLT_WRITE_TIMEOUT;

    /** Data loader flush frequency. */
    private long flushFreq = DFLT_FLUSH_FREQ;

    /** Data loader per node buffer size. */
    private int perNodeBufSize = DFLT_PER_NODE_BUF_SIZE;

    /** Data loader per node parallel load operations. */
    private int perNodeParallelLoadOps = DFLT_PARALLEL_LOAD_OPS;

    /**
     * Default constructor.
     */
    public GridDrReceiverHubConfiguration() {
        // No-op.
    }

    /**
     * Copying constructor.
     *
     * @param cfg Configuration to copy.
     */
    public GridDrReceiverHubConfiguration(GridDrReceiverHubConfiguration cfg) {
        directBuf = cfg.isDirectBuffer();
        flushFreq = cfg.getFlushFrequency();
        idleTimeout = cfg.getIdleTimeout();
        locInboundHost = cfg.getLocalInboundHost();
        locInboundPort = cfg.getLocalInboundPort();
        msgQueueLimit = cfg.getMessageQueueLimit();
        perNodeBufSize = cfg.getPerNodeBufferSize();
        perNodeParallelLoadOps = cfg.getPerNodeParallelLoadOperations();
        selectorCnt = cfg.getSelectorCount();
        tcpNodelay = cfg.isTcpNodelay();
        workerCnt = cfg.getWorkerThreads();
        writeTimeout = cfg.getWriteTimeout();
    }

    /**
     * Gets local host name receiver hub TCP server is bound to.
     * <p>
     * If not set, {@link org.apache.ignite.configuration.IgniteConfiguration#getLocalHost()} will be used.
     * <p>
     * Defaults to {@code null}.
     *
     * @return Local host name receiver hub TCP server is bound to.
     */
    public String getLocalInboundHost() {
        return locInboundHost;
    }

    /**
     * Sets local host name receiver hub TCP server is bound to. See {@link #getLocalInboundHost()} for more
     * information.
     *
     * @param locInboundHost Local host name receiver hub TCP server is bound to.
     */
    public void setLocalInboundHost(String locInboundHost) {
        this.locInboundHost = locInboundHost;
    }

    /**
     * Gets local port receiver hub TCP server is bound to.
     * <p>
     * Defaults to {@link #DFLT_LOCAL_PORT}.
     *
     * @return Local port receiver hub TCP server is bound to.
     */
    public int getLocalInboundPort() {
        return locInboundPort;
    }

    /**
     * Sets local port receiver hub TCP server is bound to. See {@link #getLocalInboundPort()} for more information.
     *
     * @param locInboundPort Local port receiver hub TCP server is bound to.
     */
    public void setLocalInboundPort(int locInboundPort) {
        this.locInboundPort = locInboundPort;
    }

    /**
     * Gets amount of selector threads in receiver hub's TCP server.
     * <p>
     * Defaults to {@link #DFLT_SELECTOR_CNT}.
     *
     * @return Amount of selector threads in receiver hub's TCP server.
     */
    public int getSelectorCount() {
        return selectorCnt;
    }

    /**
     * Sets amount of selector threads in receiver hub's TCP server. See {@link #getSelectorCount()} for more
     * information.
     *
     * @param selectorCnt Amount of selector threads in receiver hub's TCP server.
     */
    public void setSelectorCount(int selectorCnt) {
        this.selectorCnt = selectorCnt;
    }

    /**
     * Gets amount of threads responsible for sender hub batches processing.
     * <p>
     * Defaults to {@link #DFLT_WORKER_THREAD_CNT}.
     *
     * @return Amount of threads responsible for sender hub batches processing.
     */
    public int getWorkerThreads() {
        return workerCnt;
    }

    /**
     * Sets amount of threads responsible for sender hub batches processing. See {@link #getWorkerThreads()} for
     * more information.
     *
     * @param workerCnt Amount of threads responsible for sender hub batches processing.
     */
    public void setWorkerThreads(int workerCnt) {
        this.workerCnt = workerCnt;
    }

    /**
     * Gets message queue limit for incoming and outgoing messages in receiver hub's TCP server.
     * <p>
     * Defaults to {@link #DFLT_MSG_QUEUE_LIMIT}.
     *
     * @return Message queue limit for incoming and outgoing messages in receiver hub's TCP server.
     */
    public int getMessageQueueLimit() {
        return msgQueueLimit;
    }

    /**
     * Sets message queue limit for incoming and outgoing messages in receiver hub's TCP server. See
     * {@link #getMessageQueueLimit()} for more information.
     *
     * @param msgQueueLimit Message queue limit for incoming and outgoing messages in receiver hub's TCP server.
     */
    public void setMessageQueueLimit(int msgQueueLimit) {
        this.msgQueueLimit = msgQueueLimit;
    }

    /**
     * Gets TCP_NODELAY flag in receiver hub's TCP server.
     * <p>
     * Defaults to {@link #DFLT_TCP_NODELAY}.
     *
     * @return TCP_NODELAY flag in receiver hub's TCP server.
     */
    public boolean isTcpNodelay() {
        return tcpNodelay;
    }

    /**
     * Sets TCP_NODELAY flag in receiver hub's TCP server. See {@link #isTcpNodelay()} for more information.
     *
     * @param tcpNodelay TCP_NODELAY flag in receiver hub's TCP server.
     */
    public void setTcpNodelay(boolean tcpNodelay) {
        this.tcpNodelay = tcpNodelay;
    }

    /**
     * Gets direct buffer flag in receiver hub's TCP server.
     * <p>
     * Defaults to {@link #DFLT_DIRECT_BUF}.
     *
     * @return Direct buffer flag in receiver hub's TCP server.
     */
    public boolean isDirectBuffer() {
        return directBuf;
    }

    /**
     * Sets direct buffer flag in receiver hub's TCP server. See {@link #isDirectBuffer()} for more information.
     *
     * @param directBuf Direct buffer flag in receiver hub's TCP server.
     */
    public void setDirectBuffer(boolean directBuf) {
        this.directBuf = directBuf;
    }

    /**
     * Gets idle timeout for receiver hub's TCP server connection.
     * <p>
     * Defaults to {@link #DFLT_IDLE_TIMEOUT}.
     *
     * @return Idle timeout for receiver hub's TCP server connection.
     */
    public long getIdleTimeout() {
        return idleTimeout;
    }

    /**
     * Sets idle timeout for receiver hub's TCP server connection. See {@link #getWriteTimeout()} for more information.
     *
     * @param idleTimeout Idle timeout for receiver hub's TCP server connection.
     */
    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    /**
     * Gets write timeout for receiver hub's TCP server connection.
     * <p>
     * Defaults to {@link #DFLT_WRITE_TIMEOUT}.
     *
     * @return Write timeout for receiver hub's TCP server connection.
     */
    public long getWriteTimeout() {
        return writeTimeout;
    }

    /**
     * Sets write timeout for receiver hub's TCP server connection. See {@link #getWriteTimeout()} for more
     * information.
     *
     * @param writeTimeout Write timeout for receiver hub's TCP server connection.
     */
    public void setWriteTimeout(long writeTimeout) {
        this.writeTimeout = writeTimeout;
    }

    /**
     * Gets data flush frequency. Defines how often receiver hub flushes batches received from sender hubs to receiver
     * caches.
     * <p>
     * Defaults to {@link #DFLT_FLUSH_FREQ}.
     *
     * @return Data flush frequency.
     */
    public long getFlushFrequency() {
        return flushFreq;
    }

    /**
     * Sets data flush frequency. See {@link #getFlushFrequency()} for more information.
     *
     * @param flushFreq Data flush frequency.
     */
    public void setFlushFrequency(long flushFreq) {
        this.flushFreq = flushFreq;
    }

    /**
     * Gets per-node data buffer size. When amount of cache entries enqueued for particular receiver cache data node
     * exceeds this limit, pending data will be flushed to that data node.
     * <p>
     * Defaults to {@link #DFLT_PER_NODE_BUF_SIZE}.
     *
     * @return Per-node data buffer size.
     */
    public int getPerNodeBufferSize() {
        return perNodeBufSize;
    }

    /**
     * Sets per-node data buffer size. See {@link #getPerNodeBufferSize()} for more information.
     *
     * @param perNodeBufSize Per-node data buffer size.
     */
    public void setPerNodeBufferSize(int perNodeBufSize) {
        this.perNodeBufSize = perNodeBufSize;
    }

    /**
     * Gets per-node parallel load operations. Defines how many data flush operations can be performed on a single
     * receiver cache data node simultaneously.
     * <p>
     * Defaults {@link #DFLT_PARALLEL_LOAD_OPS}.
     *
     * @return Per-node parallel load operations
     */
    public int getPerNodeParallelLoadOperations() {
        return perNodeParallelLoadOps;
    }

    /**
     * Sets per-node parallel load operations. See {@link #getPerNodeParallelLoadOperations()} for more information.
     *
     * @param perNodeParallelLoadOps Per-node parallel load operations
     */
    public void setPerNodeParallelLoadOperations(int perNodeParallelLoadOps) {
        this.perNodeParallelLoadOps = perNodeParallelLoadOps;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDrReceiverHubConfiguration.class, this);
    }
}
