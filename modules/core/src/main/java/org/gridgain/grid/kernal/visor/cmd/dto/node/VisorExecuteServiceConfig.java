/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

import static org.gridgain.grid.kernal.visor.cmd.VisorTaskUtils.*;

/**
 * Data transfer object for node executors configuration properties.
 */
public class VisorExecuteServiceConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Executor service. */
    private String execSvc;

    /** Whether or not GridGain will stop executor service on node shutdown. */
    private boolean execSvcShutdown;

    /** System executor service. */
    private String sysExecSvc;

    /** Whether or not GridGain will stop system executor service on node shutdown. */
    private boolean sysExecSvcShutdown;

    /** Peer-to-peer executor service. */
    private String p2pExecSvc;

    /** Whether or not GridGain will stop peer-to-peer executor service on node shutdown. */
    private boolean p2pExecSvcShutdown;

    /**
     * @param c Grid configuration.
     * @return Data transfer object for node executors configuration properties.
     */
    public static VisorExecuteServiceConfig from(GridConfiguration c) {
        VisorExecuteServiceConfig cfg = new VisorExecuteServiceConfig();

        cfg.executeService(compactClass(c.getExecutorService()));
        cfg.executeServiceShutdown(c.getExecutorServiceShutdown());

        cfg.systemExecuteService(compactClass(c.getSystemExecutorService()));
        cfg.systemExecuteServiceShutdown(c.getSystemExecutorServiceShutdown());

        cfg.p2pExecuteService(compactClass(c.getPeerClassLoadingExecutorService()));
        cfg.p2pExecuteServiceShutdown(c.getSystemExecutorServiceShutdown());

        return cfg;
    }

    /**
     * @return Executor service.
     */
    public String executeService() {
        return execSvc;
    }

    /**
     * @param execSvc New executor service.
     */
    public void executeService(String execSvc) {
        this.execSvc = execSvc;
    }

    /**
     * @return Whether or not GridGain will stop executor service on node shutdown.
     */
    public boolean executeServiceShutdown() {
        return execSvcShutdown;
    }

    /**
     * @param execSvcShutdown New whether or not GridGain will stop executor service on node shutdown.
     */
    public void executeServiceShutdown(boolean execSvcShutdown) {
        this.execSvcShutdown = execSvcShutdown;
    }

    /**
     * @return System executor service.
     */
    public String systemExecuteService() {
        return sysExecSvc;
    }

    /**
     * @param sysExecSvc New system executor service.
     */
    public void systemExecuteService(String sysExecSvc) {
        this.sysExecSvc = sysExecSvc;
    }

    /**
     * @return Whether or not GridGain will stop system executor service on node shutdown.
     */
    public boolean systemExecuteServiceShutdown() {
        return sysExecSvcShutdown;
    }

    /**
     * @param sysExecSvcShutdown New whether or not GridGain will stop system executor service on node shutdown.
     */
    public void systemExecuteServiceShutdown(boolean sysExecSvcShutdown) {
        this.sysExecSvcShutdown = sysExecSvcShutdown;
    }

    /**
     * @return Peer-to-peer executor service.
     */
    public String p2pExecuteService() {
        return p2pExecSvc;
    }

    /**
     * @param p2pExecSvc New peer-to-peer executor service.
     */
    public void p2pExecuteService(String p2pExecSvc) {
        this.p2pExecSvc = p2pExecSvc;
    }

    /**
     * @return Whether or not GridGain will stop peer-to-peer executor service on node shutdown.
     */
    public boolean p2pExecuteServiceShutdown() {
        return p2pExecSvcShutdown;
    }

    /**
     * @param p2pExecSvcShutdown New whether or not GridGain will stop peer-to-peer executor service on node shutdown.
     */
    public void p2pExecuteServiceShutdown(boolean p2pExecSvcShutdown) {
        this.p2pExecSvcShutdown = p2pExecSvcShutdown;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorExecuteServiceConfig.class, this);
    }
}
