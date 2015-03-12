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

import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for cache preload configuration properties.
 */
public class VisorCachePreloadConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache preload mode. */
    private CacheRebalanceMode mode;

    /** Preload thread pool size. */
    private int threadPoolSize;

    /** Cache preload batch size. */
    private int batchSize;

    /** Preloading partitioned delay. */
    private long partitionedDelay;

    /** Time in milliseconds to wait between preload messages. */
    private long throttle;

    /** Preload timeout. */
    private long timeout;

    /**
     * @param ccfg Cache configuration.
     * @return Data transfer object for preload configuration properties.
     */
    public static VisorCachePreloadConfiguration from(CacheConfiguration ccfg) {
        VisorCachePreloadConfiguration cfg = new VisorCachePreloadConfiguration();

        cfg.mode = ccfg.getRebalanceMode();
        cfg.batchSize = ccfg.getRebalanceBatchSize();
        cfg.threadPoolSize = ccfg.getRebalanceThreadPoolSize();
        cfg.partitionedDelay = ccfg.getRebalanceDelay();
        cfg.throttle = ccfg.getRebalanceThrottle();
        cfg.timeout = ccfg.getRebalanceTimeout();

        return cfg;
    }

    /**
     * @return Cache preload mode.
     */
    public CacheRebalanceMode mode() {
        return mode;
    }

    /**
     * @return Preload thread pool size.
     */
    public int threadPoolSize() {
        return threadPoolSize;
    }

    /**
     * @return Cache preload batch size.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * @return Preloading partitioned delay.
     */
    public long partitionedDelay() {
        return partitionedDelay;
    }

    /**
     * @return Time in milliseconds to wait between preload messages.
     */
    public long throttle() {
        return throttle;
    }

    /**
     * @return Preload timeout.
     */
    public long timeout() {
        return timeout;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCachePreloadConfiguration.class, this);
    }
}
