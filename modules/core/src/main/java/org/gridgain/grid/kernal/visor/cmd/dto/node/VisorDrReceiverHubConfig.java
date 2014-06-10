/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import org.gridgain.grid.dr.hub.receiver.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for DR receiver hub configuration properties.
 */
public class VisorDrReceiverHubConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Local host name receiving hub server is bound to. */
    private String localInboundHost;

    /** Local port receiving hub is bound to. */
    private int localInboundPort;

    /** Number of selector threads in receiver hub's TCP server. */
    private int selectorCount;

    /** Number of threads responsible for sender hub requests processing. */
    private int workerThreads;

    /** Message queue limit for incoming and outgoing messages. */
    private int messageQueueLimit;

    /** Whether to use TCP_NODELAY mode. */
    private boolean tcpNodelay;

    /** Whether to use direct buffer when processing sender hub requests. */
    private boolean directBuffer;

    /** Idle timeout for sender hub socket connection. */
    private long idleTimeout;

    /** Write timeout for sender hub socket connection. */
    private long writeTimeout;

    /** DR data loader flush frequency. */
    private long flushFrequency;

    /** DR data loader per node buffer size. */
    private int perNodeBufferSize;

    /** Per node parallel load operations. */
    private int perNodeParallelLoadOperations;

    /**
     * @param rcvHubCfg Data center replication receiver hub configuration.
     * @return Data transfer object for DR receiver hub configuration properties.
     */
    public static VisorDrReceiverHubConfig from(GridDrReceiverHubConfiguration rcvHubCfg) {
        if (rcvHubCfg == null)
            return null;

        VisorDrReceiverHubConfig cfg = new VisorDrReceiverHubConfig();

        cfg.localInboundHost(rcvHubCfg.getLocalInboundHost());
        cfg.localInboundPort(rcvHubCfg.getLocalInboundPort());
        cfg.selectorCount(rcvHubCfg.getSelectorCount());
        cfg.workerThreads(rcvHubCfg.getWorkerThreads());
        cfg.messageQueueLimit(rcvHubCfg.getMessageQueueLimit());
        cfg.tcpNodelay(rcvHubCfg.isTcpNodelay());
        cfg.directBuffer(rcvHubCfg.isDirectBuffer());
        cfg.idleTimeout(rcvHubCfg.getIdleTimeout());
        cfg.writeTimeout(rcvHubCfg.getWriteTimeout());
        cfg.flushFrequency(rcvHubCfg.getFlushFrequency());
        cfg.perNodeBufferSize(rcvHubCfg.getPerNodeBufferSize());
        cfg.perNodeParallelLoadOperations(rcvHubCfg.getPerNodeParallelLoadOperations());

        return cfg;
    }

    /**
     * @return Local host name receiving hub server is bound to.
     */
    public String localInboundHost() {
        return localInboundHost;
    }

    /**
     * @param locInboundHost New local host name receiving hub server is bound to.
     */
    public void localInboundHost(String locInboundHost) {
        localInboundHost = locInboundHost;
    }

    /**
     * @return Local port receiving hub is bound to.
     */
    public int localInboundPort() {
        return localInboundPort;
    }

    /**
     * @param locInboundPort New local port receiving hub is bound to.
     */
    public void localInboundPort(int locInboundPort) {
        localInboundPort = locInboundPort;
    }

    /**
     * @return Number of selector threads in receiver hub's TCP server.
     */
    public int selectorCount() {
        return selectorCount;
    }

    /**
     * @param selectorCnt New number of selector threads in receiver hub's TCP server.
     */
    public void selectorCount(int selectorCnt) {
        selectorCount = selectorCnt;
    }

    /**
     * @return Number of threads responsible for sender hub requests processing.
     */
    public int workerThreads() {
        return workerThreads;
    }

    /**
     * @param workerThreads New number of threads responsible for sender hub requests processing.
     */
    public void workerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    /**
     * @return Message queue limit for incoming and outgoing messages.
     */
    public int messageQueueLimit() {
        return messageQueueLimit;
    }

    /**
     * @param msgQueueLimit New message queue limit for incoming and outgoing messages.
     */
    public void messageQueueLimit(int msgQueueLimit) {
        messageQueueLimit = msgQueueLimit;
    }

    /**
     * @return Whether to use TCP_NODELAY mode.
     */
    public boolean tcpNodelay() {
        return tcpNodelay;
    }

    /**
     * @param tcpNodelay New whether to use TCP_NODELAY mode.
     */
    public void tcpNodelay(boolean tcpNodelay) {
        this.tcpNodelay = tcpNodelay;
    }

    /**
     * @return Whether to use direct buffer when processing sender hub requests.
     */
    public boolean directBuffer() {
        return directBuffer;
    }

    /**
     * @param directBuf New whether to use direct buffer when processing sender hub requests.
     */
    public void directBuffer(boolean directBuf) {
        directBuffer = directBuf;
    }

    /**
     * @return Idle timeout for sender hub socket connection.
     */
    public long idleTimeout() {
        return idleTimeout;
    }

    /**
     * @param idleTimeout New idle timeout for sender hub socket connection.
     */
    public void idleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    /**
     * @return Write timeout for sender hub socket connection.
     */
    public long writeTimeout() {
        return writeTimeout;
    }

    /**
     * @param writeTimeout New write timeout for sender hub socket connection.
     */
    public void writeTimeout(long writeTimeout) {
        this.writeTimeout = writeTimeout;
    }

    /**
     * @return DR data loader flush frequency.
     */
    public long flushFrequency() {
        return flushFrequency;
    }

    /**
     * @param flushFreq New dR data loader flush frequency.
     */
    public void flushFrequency(long flushFreq) {
        flushFrequency = flushFreq;
    }

    /**
     * @return DR data loader per node buffer size.
     */
    public int perNodeBufferSize() {
        return perNodeBufferSize;
    }

    /**
     * @param perNodeBufSize New dR data loader per node buffer size.
     */
    public void perNodeBufferSize(int perNodeBufSize) {
        perNodeBufferSize = perNodeBufSize;
    }

    /**
     * @return Per node parallel load operations.
     */
    public int perNodeParallelLoadOperations() {
        return perNodeParallelLoadOperations;
    }

    /**
     * @param perNodeParallelLoadOperations New per node parallel load operations.
     */
    public void perNodeParallelLoadOperations(int perNodeParallelLoadOperations) {
        this.perNodeParallelLoadOperations = perNodeParallelLoadOperations;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrReceiverHubConfig.class, this);
    }
}
