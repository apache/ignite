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

import java.io.Serializable;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data transfer object for cache rebalance configuration properties.
 */
public class VisorCacheRebalanceConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache rebalance mode. */
    private CacheRebalanceMode mode;

    /** Rebalance thread pool size. */
    private int threadPoolSize;

    /** Cache rebalance batch size. */
    private int batchSize;

    /** Rebalance partitioned delay. */
    private long partitionedDelay;

    /** Time in milliseconds to wait between rebalance messages. */
    private long throttle;

    /** Rebalance timeout. */
    private long timeout;

    /**
     * @param ccfg Cache configuration.
     * @return Data transfer object for rebalance configuration properties.
     */
    public static VisorCacheRebalanceConfiguration from(CacheConfiguration ccfg) {
        VisorCacheRebalanceConfiguration cfg = new VisorCacheRebalanceConfiguration();

        cfg.mode = ccfg.getRebalanceMode();
        cfg.batchSize = ccfg.getRebalanceBatchSize();
        cfg.threadPoolSize = ccfg.getRebalanceThreadPoolSize();
        cfg.partitionedDelay = ccfg.getRebalanceDelay();
        cfg.throttle = ccfg.getRebalanceThrottle();
        cfg.timeout = ccfg.getRebalanceTimeout();

        return cfg;
    }

    /**
     * @return Cache rebalance mode.
     */
    public CacheRebalanceMode mode() {
        return mode;
    }

    /**
     * @return Rebalance thread pool size.
     */
    public int threadPoolSize() {
        return threadPoolSize;
    }

    /**
     * @return Cache rebalance batch size.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * @return Rebalance partitioned delay.
     */
    public long partitionedDelay() {
        return partitionedDelay;
    }

    /**
     * @return Time in milliseconds to wait between rebalance messages.
     */
    public long throttle() {
        return throttle;
    }

    /**
     * @return Rebalance timeout.
     */
    public long timeout() {
        return timeout;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheRebalanceConfiguration.class, this);
    }
}