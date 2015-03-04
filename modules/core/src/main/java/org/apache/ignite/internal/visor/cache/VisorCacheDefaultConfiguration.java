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

package org.apache.ignite.internal.visor.cache;

import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.transactions.*;

import java.io.*;

/**
 * Data transfer object for default cache configuration properties.
 */
public class VisorCacheDefaultConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default transaction isolation. */
    private TransactionIsolation txIsolation;

    /** Default transaction concurrency. */
    private TransactionConcurrency txConcurrency;

    /** TTL value. */
    private long ttl;

    /** Default transaction concurrency. */
    private long txTimeout;

    /** Default transaction timeout. */
    private long txLockTimeout;

    /** Default query timeout. */
    private long qryTimeout;

    /**
     * @param ccfg Cache configuration.
     * @return Data transfer object for default cache configuration properties.
     */
    public static VisorCacheDefaultConfiguration from(CacheConfiguration ccfg) {
        // TODO GG-9141 Update Visor.

        VisorCacheDefaultConfiguration cfg = new VisorCacheDefaultConfiguration();

//        cfg.txIsolation(ccfg.getDefaultTxIsolation());
//        cfg.txConcurrency(ccfg.getDefaultTxConcurrency());
        cfg.timeToLive(ccfg.getDefaultTimeToLive());
//        cfg.txTimeout(ccfg.getDefaultTxTimeout());
        cfg.txLockTimeout(ccfg.getDefaultLockTimeout());
        cfg.queryTimeout(ccfg.getDefaultQueryTimeout());

        return cfg;
    }

    /**
     * @return Default transaction isolation.
     */
    public TransactionIsolation txIsolation() {
        return txIsolation;
    }

    /**
     * @param txIsolation New default transaction isolation.
     */
    public void txIsolation(TransactionIsolation txIsolation) {
        this.txIsolation = txIsolation;
    }

    /**
     * @return Default transaction concurrency.
     */
    public TransactionConcurrency txConcurrency() {
        return txConcurrency;
    }

    /**
     * @param txConcurrency New default transaction concurrency.
     */
    public void txConcurrency(TransactionConcurrency txConcurrency) {
        this.txConcurrency = txConcurrency;
    }

    /**
     * @return TTL value.
     */
    public long timeToLive() {
        return ttl;
    }

    /**
     * @param ttl New tTL value.
     */
    public void timeToLive(long ttl) {
        this.ttl = ttl;
    }

    /**
     * @return Default transaction concurrency.
     */
    public long txTimeout() {
        return txTimeout;
    }

    /**
     * @param txTimeout New default transaction concurrency.
     */
    public void txTimeout(long txTimeout) {
        this.txTimeout = txTimeout;
    }

    /**
     * @return Default transaction timeout.
     */
    public long txLockTimeout() {
        return txLockTimeout;
    }

    /**
     * @param txLockTimeout New default transaction timeout.
     */
    public void txLockTimeout(long txLockTimeout) {
        this.txLockTimeout = txLockTimeout;
    }

    /**
     * @return Default query timeout.
     */
    public long queryTimeout() {
        return qryTimeout;
    }

    /**
     * @param qryTimeout New default query timeout.
     */
    public void queryTimeout(long qryTimeout) {
        this.qryTimeout = qryTimeout;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheDefaultConfiguration.class, this);
    }
}
