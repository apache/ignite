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

package org.apache.ignite.internal.processors.cache.database;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DatabaseConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.file.MappedFileMemoryProvider;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.File;

/**
 *
 */
public class IgniteCacheDatabaseSharedManager extends GridCacheSharedManagerAdapter {
    /** */
    protected PageMemory pageMem;

    /** */
    protected MetadataStorage meta;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        DatabaseConfiguration dbCfg = cctx.kernalContext().config().getDatabaseConfiguration();

        if (!cctx.kernalContext().clientNode()) {
            if (dbCfg == null)
                dbCfg = new DatabaseConfiguration();

            pageMem = initMemory(dbCfg);

            pageMem.start();

            meta = new MetadataStorage(pageMem, cctx.wal());
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        if (pageMem != null)
            pageMem.stop();
    }

    /**
     *
     */
    public boolean persistenceEnabled() {
        return false;
    }

    /**
     * @return Page memory instance.
     */
    public PageMemory pageMemory() {
        return pageMem;
    }

    /**
     * @return Metadata storage.
     */
    public MetadataStorage meta() {
        return meta;
    }

    /**
     * No-op for non-persistent storage.
     */
    public void checkpointReadLock() {
        // No-op.
    }

    /**
     * No-op for non-persistent storage.
     */
    public void checkpointReadUnlock() {
        // No-op.
    }

    /**
     * @param discoEvt Before exchange for the given discovery event.
     */
    public void beforeExchange(DiscoveryEvent discoEvt) throws IgniteCheckedException {
        // No-op.
    }

    /**
     * @param dbCfg Database configuration.
     * @return Page memory instance.
     */
    protected PageMemory initMemory(DatabaseConfiguration dbCfg) {
        String path = dbCfg.getFileCacheAllocationPath();

        int concLvl = dbCfg.getConcurrencyLevel();

        if (concLvl < 2)
            concLvl = Runtime.getRuntime().availableProcessors();

        long fragmentSize = dbCfg.getPageCacheSize() / concLvl;

        if (fragmentSize < 1024 * 1024)
            fragmentSize = 1024 * 1024;

        String consId = String.valueOf(cctx.discovery().consistentId());

        consId = consId.replaceAll("[:,\\.]", "_");

        File allocPath = path == null ? null : buildPath(path, consId);

        long[] sizes = new long[concLvl];

        for (int i = 0; i < concLvl; i++)
            sizes[i] = fragmentSize;

        DirectMemoryProvider memProvider = path == null ?
            new UnsafeMemoryProvider(sizes) :
            new MappedFileMemoryProvider(
                log,
                allocPath,
                true,
                sizes);

        return new PageMemoryNoStoreImpl(log, memProvider, cctx, dbCfg.getPageSize());
    }

    /**
     * @param path Path to the working directory.
     * @param consId Consistent ID of the local node.
     * @return DB storage path.
     */
    protected File buildPath(String path, String consId) {
        String igniteHomeStr = U.getIgniteHome();

        File igniteHome = igniteHomeStr != null ? new File(igniteHomeStr) : null;

        File workDir = igniteHome == null ? new File(path) : new File(igniteHome, path);

        return new File(workDir, consId);
    }}
