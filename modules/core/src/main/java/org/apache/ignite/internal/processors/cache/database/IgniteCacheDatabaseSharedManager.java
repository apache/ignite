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
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.file.MappedFileMemoryProvider;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 */
public class IgniteCacheDatabaseSharedManager extends GridCacheSharedManagerAdapter {
    /** */
    private PageMemory pageMem;

    /** */
    private MetadataStorage meta;

    /** */
    private ReadWriteLock txLock = new ReentrantReadWriteLock();

    /** {@inheritDoc} */
    @Override public void onKernalStart0(boolean reconnect) throws IgniteCheckedException {
        DatabaseConfiguration dbCfg = cctx.kernalContext().config().getDatabaseConfiguration();

        if (dbCfg != null && !cctx.kernalContext().clientNode()) {
            pageMem = createPageMemory(dbCfg);

            pageMem.start();

            meta = new MetadataStorage(pageMem);
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        if (pageMem != null)
            pageMem.stop();
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
     * Callback invoked before transaction is committed.
     *
     * @param xidVer Transaction version.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    public void txCommitBegin(GridCacheVersion xidVer) {
        txLock.readLock().lock();
    }

    /**
     * Callback invoked after transaction has been committed.
     *
     * @param xidVer Transaction version.
     */
    public void txCommitEnd(GridCacheVersion xidVer) {
        txLock.readLock().unlock();
    }

    /**
     * Marks checkpoint begin.
     */
    public Collection<FullPageId> snapshotCheckpoint() {
        txLock.writeLock().lock();

        try {
            return pageMem.beginCheckpoint();
        }
        finally {
            txLock.writeLock().unlock();
        }
    }

    /**
     * Marks checkpoint end.
     */
    public void checkpointEnd() {
        pageMem.finishCheckpoint();
    }

    /**
     * @param dbCfg Database configuration.
     * @return Page memory instance.
     */
    private PageMemory createPageMemory(DatabaseConfiguration dbCfg) {
        String path = dbCfg.getFileCacheAllocationPath();

        long fragmentSize = dbCfg.getFragmentSize();

        if (fragmentSize == 0)
            fragmentSize = dbCfg.getPageCacheSize();

        String consId = String.valueOf(cctx.discovery().localNode().consistentId());

        consId = consId.replaceAll("[:,\\.]", "_");

        File allocPath = path == null ? null : buildPath(path, consId);

        boolean clean = true;

        if (allocPath != null) {
            allocPath.mkdirs();

            File[] files = allocPath.listFiles();

            clean = files == null || files.length == 0;
        }

        DirectMemoryProvider memProvider = path == null ?
            new UnsafeMemoryProvider(dbCfg.getPageCacheSize(), fragmentSize) :
            new MappedFileMemoryProvider(
                log,
                allocPath,
                clean,
                dbCfg.getPageCacheSize(),
                fragmentSize);

        return new PageMemoryImpl(log, memProvider, cctx.pageStore(), dbCfg.getPageSize(), dbCfg.getConcurrencyLevel());
    }

    /**
     * @param path Path to the working directory.
     * @param consId Consistent ID of the local node.
     * @return DB storage path.
     */
    private File buildPath(String path, String consId) {
        String igniteHomeStr = U.getIgniteHome();

        File igniteHome = igniteHomeStr != null ? new File(igniteHomeStr) : null;

        File workDir = igniteHome == null ? new File(path) : new File(igniteHome, path);

        return new File(workDir, consId);
    }}
