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

import java.time.Duration;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.jetbrains.annotations.Nullable;

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

    /** Dump name. */
    @Nullable private final String name;

    /** Root dump directory. */
    @Nullable private final String path;

    /** Optional Ignite configuration. */
    @Nullable private final IgniteConfiguration cfg;

    /** Dump consumer. */
    private final DumpConsumer cnsmr;

    /** Count of threads to consume dumped partitions. */
    private final int thCnt;

    /** Timeout of dump reader. 1 week, by default. */
    private final Duration timeout;

    /** Stop processing partitions if consumer fail to process one. */
    private final boolean failFast;

    /**
     * If {@code true} and if {@link #keepRaw} is {@code false} then keeps {@link DumpEntry#key()} and
     * {@link DumpEntry#value()} as {@link BinaryObject}.
     */
    private final boolean keepBinary;

    /**
     * If {@code true}, doesn't deserialize cache data and keeps {@link DumpEntry#key()} as {@link KeyCacheObject} and
     * {@link DumpEntry#value()} as {@link CacheObject}. If {@code true}, disables {@link #keepBinary}.
     */
    private final boolean keepRaw;

    /** Cache group names. */
    private final String[] cacheGrpNames;

    /** Skip copies. */
    private final boolean skipCopies;

    /** Encryption SPI. */
    private final EncryptionSpi encSpi;

    /**
     * @param path Dump path.
     * @param cnsmr Dump consumer.
     */
    public DumpReaderConfiguration(String name, @Nullable String path, @Nullable IgniteConfiguration cfg, DumpConsumer cnsmr) {
        this(name, path, cfg, cnsmr, DFLT_THREAD_CNT, DFLT_TIMEOUT, true, true, false, null, false, null);
    }

    /**
     * @param name Root dump directory.
     * @param cnsmr Dump consumer.
     * @param thCnt Count of threads to consume dumped partitions.
     * @param timeout Timeout of dump reader invocation.
     * @param failFast Stop processing partitions if consumer fail to process one.
     * @param keepBinary If {@code true} and if {@link #keepRaw} is {@code false} then keeps {@link DumpEntry#key()} and
     *                   {@link DumpEntry#value()} as {@link BinaryObject}.
     * @param keepRaw If {@code true}, doesn't deserialize cache data and keeps {@link DumpEntry#key()} as
     *                {@link KeyCacheObject} and {@link DumpEntry#value()} as {@link CacheObject}. If {@code true},
     *                disables {@link #keepBinary}.
     * @param cacheGrpNames Cache group names.
     * @param skipCopies Skip copies.
     * @param encSpi Encryption SPI.
     */
    public DumpReaderConfiguration(
        @Nullable String name,
        @Nullable String path,
        @Nullable IgniteConfiguration cfg,
        DumpConsumer cnsmr,
        int thCnt,
        Duration timeout,
        boolean failFast,
        boolean keepBinary,
        boolean keepRaw,
        String[] cacheGrpNames,
        boolean skipCopies,
        EncryptionSpi encSpi
    ) {
        this.name = name;
        this.path = path;
        this.cfg = cfg;
        this.cnsmr = cnsmr;
        this.thCnt = thCnt;
        this.timeout = timeout;
        this.failFast = failFast;
        this.keepBinary = keepBinary;
        this.keepRaw = keepRaw;
        this.cacheGrpNames = cacheGrpNames;
        this.skipCopies = skipCopies;
        this.encSpi = encSpi;
    }

    /** @return Root dump name. */
    public @Nullable String dumpName() {
        return name;
    }

    /** @return Root dump directiory. */
    public @Nullable String dumpRoot() {
        return path;
    }

    /** @return Ignite configuration. */
    public @Nullable IgniteConfiguration config() {
        return cfg;
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

    /**
     * Actual only if {@link #keepRaw} is {@code false}.
     *
     * @return {@code True} if {@link DumpEntry#key()} and {@link DumpEntry#value()} are kept as {@link BinaryObject}.
     */
    public boolean keepBinary() {
        return keepBinary;
    }

    /**
     * @return {@code True} if {@link DumpEntry#key()} and {@link DumpEntry#value()} are kept as {@link KeyCacheObject}
     * and {@link CacheObject} respectively.
     */
    public boolean keepRaw() {
        return keepRaw;
    }

    /** @return Cache group names. */
    public String[] cacheGroupNames() {
        return cacheGrpNames;
    }

    /** @return Skip copies. */
    public boolean skipCopies() {
        return skipCopies;
    }

    /** @return Encryption SPI */
    public EncryptionSpi encryptionSpi() {
        return encSpi;
    }
}
