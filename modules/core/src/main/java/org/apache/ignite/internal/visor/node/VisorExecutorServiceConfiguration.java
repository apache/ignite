/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.node;

import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

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

        ClientConnectionConfiguration cc = c.getClientConnectionConfiguration();

        if (cc != null) {
            cfg.restExecutorService(compactClass(cc.getExecutorService()));
            cfg.restExecutorServiceShutdown(cc.isExecutorServiceShutdown());
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
