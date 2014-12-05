/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.node;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Data transfer object for node executors configuration properties.
 */
public class VisorExecutorServiceConfiguration implements Serializable {
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

    /** REST requests executor service. */
    private String restExecSvc;

    /** REST executor service shutdown flag. */
    private boolean restSvcShutdown;

    /**
     * @param c Grid configuration.
     * @return Data transfer object for node executors configuration properties.
     */
    public static VisorExecutorServiceConfiguration from(IgniteConfiguration c) {
        VisorExecutorServiceConfiguration cfg = new VisorExecutorServiceConfiguration();

        cfg.executeService(compactClass(c.getExecutorService()));
        cfg.executeServiceShutdown(c.getExecutorServiceShutdown());

        cfg.systemExecutorService(compactClass(c.getSystemExecutorService()));
        cfg.systemExecutorServiceShutdown(c.getSystemExecutorServiceShutdown());

        cfg.p2pExecutorService(compactClass(c.getPeerClassLoadingExecutorService()));
        cfg.p2pExecutorServiceShutdown(c.getSystemExecutorServiceShutdown());

        GridClientConnectionConfiguration cc = c.getClientConnectionConfiguration();

        if (cc != null) {
            cfg.restExecutorService(compactClass(cc.getRestExecutorService()));
            cfg.restExecutorServiceShutdown(cc.isRestExecutorServiceShutdown());
        }

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
    public String systemExecutorService() {
        return sysExecSvc;
    }

    /**
     * @param sysExecSvc New system executor service.
     */
    public void systemExecutorService(String sysExecSvc) {
        this.sysExecSvc = sysExecSvc;
    }

    /**
     * @return Whether or not GridGain will stop system executor service on node shutdown.
     */
    public boolean systemExecutorServiceShutdown() {
        return sysExecSvcShutdown;
    }

    /**
     * @param sysExecSvcShutdown New whether or not GridGain will stop system executor service on node shutdown.
     */
    public void systemExecutorServiceShutdown(boolean sysExecSvcShutdown) {
        this.sysExecSvcShutdown = sysExecSvcShutdown;
    }

    /**
     * @return Peer-to-peer executor service.
     */
    public String p2pExecutorService() {
        return p2pExecSvc;
    }

    /**
     * @param p2pExecSvc New peer-to-peer executor service.
     */
    public void p2pExecutorService(String p2pExecSvc) {
        this.p2pExecSvc = p2pExecSvc;
    }

    /**
     * @return Whether or not GridGain will stop peer-to-peer executor service on node shutdown.
     */
    public boolean p2pExecutorServiceShutdown() {
        return p2pExecSvcShutdown;
    }

    /**
     * @param p2pExecSvcShutdown New whether or not GridGain will stop peer-to-peer executor service on node shutdown.
     */
    public void p2pExecutorServiceShutdown(boolean p2pExecSvcShutdown) {
        this.p2pExecSvcShutdown = p2pExecSvcShutdown;
    }

    /**
     * @return REST requests executor service.
     */
    public String restExecutorService() {
        return restExecSvc;
    }

    /**
     * @param restExecSvc New REST requests executor service.
     */
    public void restExecutorService(String restExecSvc) {
        this.restExecSvc = restExecSvc;
    }

    /**
     * @return REST executor service shutdown flag.
     */
    public boolean restExecutorServiceShutdown() {
        return restSvcShutdown;
    }

    /**
     * @param restSvcShutdown New REST executor service shutdown flag.
     */
    public void restExecutorServiceShutdown(boolean restSvcShutdown) {
        this.restSvcShutdown = restSvcShutdown;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorExecutorServiceConfiguration.class, this);
    }
}
