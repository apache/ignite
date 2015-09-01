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

package org.apache.ignite.yardstick;

import com.beust.jcommander.Parameter;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Input arguments for Ignite benchmarks.
 */
@SuppressWarnings({"UnusedDeclaration", "FieldCanBeLocal"})
public class IgniteBenchmarkArguments {
    /** */
    @Parameter(names = {"-nn", "--nodeNumber"}, description = "Node number")
    private int nodes = 1;

    /** */
    @Parameter(names = {"-b", "--backups"}, description = "Backups")
    private int backups;

    @Parameter(names = {"-cfg", "--Config"}, description = "Configuration file")
    private String cfg = "config/ignite-localhost-config.xml";

    /** */
    @Parameter(names = {"-sm", "--syncMode"}, description = "Synchronization mode")
    private CacheWriteSynchronizationMode syncMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;

    /** */
    @Parameter(names = {"-cl", "--client"}, description = "Client flag")
    private boolean clientOnly = false;

    /** */
    @Parameter(names = {"-nc", "--nearCache"}, description = "Near cache flag")
    private boolean nearCacheFlag = false;

    /** */
    @Parameter(names = {"-wom", "--writeOrderMode"}, description = "Write ordering mode")
    private CacheAtomicWriteOrderMode orderMode;

    /** */
    @Parameter(names = {"-txc", "--txConcurrency"}, description = "Transaction concurrency")
    private TransactionConcurrency txConcurrency = TransactionConcurrency.OPTIMISTIC;

    /** */
    @Parameter(names = {"-txi", "--txIsolation"}, description = "Transaction isolation")
    private TransactionIsolation txIsolation = TransactionIsolation.REPEATABLE_READ;

    /** */
    @Parameter(names = {"-ot", "--offheapTiered"}, description = "Tiered offheap")
    private boolean offheapTiered;

    /** */
    @Parameter(names = {"-ov", "--offheapValuesOnly"}, description = "Offheap values only")
    private boolean offheapVals;

    /** */
    @Parameter(names = {"-rtp", "--restPort"}, description = "REST TCP port")
    private int restTcpPort;

    /** */
    @Parameter(names = {"-rth", "--restHost"}, description = "REST TCP host")
    private String restTcpHost;

    /** */
    @Parameter(names = {"-ss", "--syncSend"}, description = "Synchronous send")
    private boolean syncSnd;

    /** */
    @Parameter(names = {"-r", "--range"}, description = "Key range")
    private int range = 1_000_000;

    /** */
    @Parameter(names = {"-j", "--jobs"}, description = "Number of jobs for compute benchmarks")
    private int jobs = 10;

    /** */
    @Parameter(names = {"-cs", "--cacheStore"}, description = "Enable or disable cache store readThrough, writeThrough")
    private boolean storeEnabled;

    /** */
    @Parameter(names = {"-wb", "--writeBehind"}, description = "Enable or disable writeBehind for cache store")
    private boolean writeBehind;

    /** */
    @Parameter(names = {"-bs", "--batchSize"}, description = "Batch size")
    private int batch = 500;

    /** */
    @Parameter(names = {"-col", "--collocated"}, description = "Collocated")
    private boolean collocated;

    /** */
    @Parameter(names = {"-jdbc", "--jdbcUrl"}, description = "JDBC url")
    private String jdbcUrl;

    /**
     * @return JDBC url.
     */
    public String jdbcUrl() {
        return jdbcUrl;
    }

    /**
     * @return Transaction concurrency.
     */
    public TransactionConcurrency txConcurrency() {
        return txConcurrency;
    }

    /**
     * @return Transaction isolation.
     */
    public TransactionIsolation txIsolation() {
        return txIsolation;
    }

    /**
     * @return REST TCP port.
     */
    public int restTcpPort() {
        return restTcpPort;
    }

    /**
     * @return REST TCP host.
     */
    public String restTcpHost() {
        return restTcpHost;
    }

    /**
     * @return Distribution.
     */
    public boolean isClientOnly() {
        return clientOnly;
    }

    /**
     * @return Near cache flag.
     */
    public boolean isNearCache() {
        return nearCacheFlag;
    }

    /**
     * @return Synchronization.
     */
    public CacheWriteSynchronizationMode syncMode() {
        return syncMode;
    }

    /**
     * @return Cache write ordering mode.
     */
    public CacheAtomicWriteOrderMode orderMode() {
        return orderMode;
    }

    /**
     * @return Backups.
     */
    public int backups() {
        return backups;
    }

    /**
     * @return Offheap tiered.
     */
    public boolean isOffheapTiered() {
        return offheapTiered;
    }

    /**
     * @return Offheap values.
     */
    public boolean isOffheapValues() {
        return offheapVals;
    }

    /**
     * @return {@code True} if any offheap is enabled.
     */
    public boolean isOffHeap() {
        return offheapTiered || offheapVals;
    }

    /**
     * @return Nodes.
     */
    public int nodes() {
        return nodes;
    }

    /**
     * @return {@code True} if sending is synchronous.
     */
    public boolean isSyncSend() {
        return syncSnd;
    }

    /**
     * @return Key range, from {@code 0} to this number.
     */
    public int range() {
        return range;
    }

    /**
     * @return Configuration file.
     */
    public String configuration() {
        return cfg;
    }

    /**
     * @return Number of jobs
     */
    public int jobs() {
        return jobs;
    }

    /**
     * @return {@code True} if enabled readThrough, writeThrough for cache.
     */
    public boolean isStoreEnabled() {
        return storeEnabled;
    }

    /**
     * @return {@code True} if enabled writeBehind for cache store.
     */
    public boolean isWriteBehind() {
        return writeBehind;
    }

    /**
     * @return Batch size.
     */
    public int batch() {
        return batch;
    }

    /**
     * @return Collocated.
     */
    public boolean collocated() {
        return collocated;
    }

    /**
     * @return Description.
     */
    public String description() {
        return "-nn=" + nodes + "-b=" + backups + "-sm=" + syncMode + "-cl=" + clientOnly + "-nc=" + nearCacheFlag +
            (orderMode == null ? "" : "-wom=" + orderMode) + "-txc=" + txConcurrency;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(IgniteBenchmarkArguments.class, this);
    }
}