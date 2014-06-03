/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import java.io.*;

/**
 * Data transfer object for node executors configuration properties.
 */
public class VisorExecServiceConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /**Executor service. */
    private final String execSvc;
    /**Whether or not GridGain will stop executor service on node shutdown. */
    private final boolean execSvcShutdown;

    /**System executor service. */
    private final String sysExecSvc;
    /**Whether or not GridGain will stop system executor service on node shutdown. */
    private final boolean sysExecSvcShutdown;

    /**Peer-to-peer executor service. */
    private final String p2pExecSvc;
    /**Whether or not GridGain will stop peer-to-peer executor service on node shutdown. */
    private final boolean p2pExecSvcShutdown;

    /** Create data transfer object with given parameters. */
    public VisorExecServiceConfig(
        String execSvc,
        boolean execSvcShutdown,
        String sysExecSvc,
        boolean sysExecSvcShutdown,
        String p2pExecSvc,
        boolean p2pExecSvcShutdown
    ) {
        this.execSvc = execSvc;
        this.execSvcShutdown = execSvcShutdown;

        this.sysExecSvc = sysExecSvc;
        this.sysExecSvcShutdown = sysExecSvcShutdown;

        this.p2pExecSvc = p2pExecSvc;
        this.p2pExecSvcShutdown = p2pExecSvcShutdown;
    }

    /**
     * @return Executor service.
     */
    public String executorService() {
        return execSvc;
    }

    /**
     * @return Whether or not GridGain will stop executor service on node shutdown.
     */
    public boolean executorServiceShutdown() {
        return execSvcShutdown;
    }

    /**
     * @return System executor service.
     */
    public String systemExecutorService() {
        return sysExecSvc;
    }

    /**
     * @return Whether or not GridGain will stop system executor service on node shutdown.
     */
    public boolean systemExecutorServiceShutdown() {
        return sysExecSvcShutdown;
    }

    /**
     * @return Peer-to-peer executor service.
     */
    public String p2pExecutorService() {
        return p2pExecSvc;
    }

    /**
     * @return Whether or not GridGain will stop peer-to-peer executor service on node shutdown.
     */
    public boolean p2pExecutorServiceShutdown() {
        return p2pExecSvcShutdown;
    }
}
