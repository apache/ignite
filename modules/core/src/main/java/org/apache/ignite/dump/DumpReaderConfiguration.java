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

package org.apache.ignite.dump;

import java.io.File;
import java.time.Duration;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Configuration class of {@link DumpReader}.
 *
 * @see DumpReader
 * @see DumpConsumer
 */
@IgniteExperimental
public class DumpReaderConfiguration {
    /** Default timeout. */
    public static final Duration DFLT_TIMEOUT = Duration.ofDays(7);

    /** Default thread count. */
    public static final int DFLT_THREAD_CNT = 1;

    /** Root dump directory. */
    private final File dir;

    /** Dump consumer. */
    private final DumpConsumer cnsmr;

    /** Count of threads to consume dumped partitions. */
    private final int thCnt;

    /** Timeout of dump reader. 1 week, by default. */
    private final Duration timeout;

    /** Stop processing partitions if consumer fail to process one. */
    private final boolean failFast;

    /** If {@code true} then don't deserialize {@link KeyCacheObject} and {@link CacheObject}. */
    private final boolean keepBinary;

    /** Cache group names. */
    private String[] cacheGroupNames;

    /** Skip copies. */
    private final boolean skipCopies;

    /**
     * @param dir Root dump directory.
     * @param cnsmr Dump consumer.
     */
    public DumpReaderConfiguration(File dir, DumpConsumer cnsmr) {
        this(dir, cnsmr, DFLT_THREAD_CNT, DFLT_TIMEOUT, true, true, null, false);
    }

    /**
     * @param dir Root dump directory.
     * @param cnsmr Dump consumer.
     * @param thCnt Count of threads to consume dumped partitions.
     * @param timeout Timeout of dump reader invocation.
     * @param failFast Stop processing partitions if consumer fail to process one.
     * @param keepBinary If {@code true} then don't deserialize {@link KeyCacheObject} and {@link CacheObject}.
     * @param cacheGroupNames Cache group names.
     */
    public DumpReaderConfiguration(File dir,
        DumpConsumer cnsmr,
        int thCnt,
        Duration timeout,
        boolean failFast,
        boolean keepBinary,
        String[] cacheGroupNames
    ) {
        this(dir, cnsmr, thCnt, timeout, failFast, keepBinary, cacheGroupNames, false);
    }

    /**
     * @param dir Root dump directory.
     * @param cnsmr Dump consumer.
     * @param thCnt Count of threads to consume dumped partitions.
     * @param timeout Timeout of dump reader invocation.
     * @param failFast Stop processing partitions if consumer fail to process one.
     * @param keepBinary If {@code true} then don't deserialize {@link KeyCacheObject} and {@link CacheObject}.
     * @param cacheGroupNames Cache group names.
     * @param skipCopies Skip copies.
     */
    public DumpReaderConfiguration(File dir,
        DumpConsumer cnsmr,
        int thCnt,
        Duration timeout,
        boolean failFast,
        boolean keepBinary,
        String[] cacheGroupNames,
        boolean skipCopies
    ) {
        this.dir = dir;
        this.cnsmr = cnsmr;
        this.thCnt = thCnt;
        this.timeout = timeout;
        this.failFast = failFast;
        this.keepBinary = keepBinary;
        this.cacheGroupNames = cacheGroupNames;
        this.skipCopies = skipCopies;
    }

    /** @return Root dump directiory. */
    public File dumpRoot() {
        return dir;
    }

    /** @return Dump consumer instance. */
    public DumpConsumer consumer() {
        return cnsmr;
    }

    /** @return Count of threads to consume dumped partitions. */
    public int threadCount() {
        return thCnt;
    }

    /** @return Timeout of dump reader invocation. */
    public Duration timeout() {
        return timeout;
    }

    /** @return {@code True} if stop processing after first consumer error. */
    public boolean failFast() {
        return failFast;
    }

    /** @return If {@code true} then don't deserialize {@link KeyCacheObject} and {@link CacheObject}. */
    public boolean keepBinary() {
        return keepBinary;
    }

    /** @return Cache group names. */
    public String[] cacheGroupNames() {
        return cacheGroupNames;
    }

    /** @return Skip copies. */
    public boolean skipCopies() {
        return skipCopies;
    }
}
