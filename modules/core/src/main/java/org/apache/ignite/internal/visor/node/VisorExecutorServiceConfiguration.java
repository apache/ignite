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

/**
 * Data transfer object for node executors configuration properties.
 */
public class VisorExecutorServiceConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Public pool size. */
    private int execPoolSz;

    /** System pool size. */
    private int sysPoolSz;

    /** Peer-to-peer pool size. */
    private int p2pPoolSz;

    /** REST requests pool size. */
    private int restPoolSz;

    /**
     * @param c Grid configuration.
     * @return Data transfer object for node executors configuration properties.
     */
    public static VisorExecutorServiceConfiguration from(IgniteConfiguration c) {
        VisorExecutorServiceConfiguration cfg = new VisorExecutorServiceConfiguration();

        cfg.executeService(c.getPublicThreadPoolSize());

        cfg.systemExecutorService(c.getSystemThreadPoolSize());

        cfg.p2pExecutorService(c.getPeerClassLoadingThreadPoolSize());

        ClientConnectionConfiguration cc = c.getClientConnectionConfiguration();

        if (cc != null)
            cfg.restExecutorService(cc.getRestThreadPoolSize());

        return cfg;
    }

    /**
     * @return Public pool size.
     */
    public int executeService() {
        return execPoolSz;
    }

    /**
     * @param execPoolSz Public pool size.
     */
    public void executeService(int execPoolSz) {
        this.execPoolSz = execPoolSz;
    }

    /**
     * @return System pool size.
     */
    public int systemExecutorService() {
        return sysPoolSz;
    }

    /**
     * @param sysExecSvc System pool size.
     */
    public void systemExecutorService(int sysExecSvc) {
        this.sysPoolSz = sysExecSvc;
    }

    /**
     * @return Peer-to-peer pool size.
     */
    public int p2pExecutorService() {
        return p2pPoolSz;
    }

    /**
     * @param p2pExecSvc New peer-to-peer pool size.
     */
    public void p2pExecutorService(int p2pExecSvc) {
        this.p2pPoolSz = p2pExecSvc;
    }

    /**
     * @param restPoolSz REST requests pool size.
     */
    public void restExecutorService(int restPoolSz) {
        this.restPoolSz = restPoolSz;
    }

    /**
     * @return REST requests pool size.
     */
    public int restExecutorService() {
        return restPoolSz;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorExecutorServiceConfiguration.class, this);
    }
}
