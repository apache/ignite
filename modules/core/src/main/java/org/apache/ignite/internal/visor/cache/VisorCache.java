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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.cache.CachePeekMode.ONHEAP;
import static org.apache.ignite.cache.CachePeekMode.PRIMARY;
import static org.apache.ignite.cache.CachePeekMode.BACKUP;

/**
 * Data transfer object for {@link IgniteCache}.
 */
public class VisorCache extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final CachePeekMode[] PEEK_ONHEAP_PRIMARY =
        new CachePeekMode[] {ONHEAP, PRIMARY};

    /** */
    private static final CachePeekMode[] PEEK_ONHEAP_BACKUP =
        new CachePeekMode[] {ONHEAP, BACKUP};

    /** Cache name. */
    private String name;

    /** Cache deployment ID. */
    private IgniteUuid dynamicDeploymentId;

    /** Cache mode. */
    private CacheMode mode;

    /** Cache size in bytes. */
    private long memorySize;

    /** Cache size in bytes. */
    private long indexesSize;

    /** Number of all entries in cache. */
    private long size;

    /** Number of all entries in near cache. */
    private int nearSize;

    /** Number of primary entries in cache. */
    private long primarySize;

    /** Number of backup entries in cache. */
    private long backupSize;

    /** Number of partitions. */
    private int partitions;

    /** Flag indicating that cache has near cache. */
    private boolean near;

    /** Cache metrics. */
    private VisorCacheMetrics metrics;

    /**
     * Create data transfer object for given cache.
     */
    public VisorCache() {
        // No-op.
    }

    /**
     * Create data transfer object for given cache.
     *
     * @param ca Internal cache.
     * @throws IgniteCheckedException If failed to create data transfer object.
     */
    public VisorCache(IgniteEx ignite, GridCacheAdapter ca) throws IgniteCheckedException {
        assert ca != null;

        GridCacheContext cctx = ca.context();
        CacheConfiguration cfg = ca.configuration();

        name = ca.name();
        dynamicDeploymentId = cctx.dynamicDeploymentId();
        mode = cfg.getCacheMode();

        primarySize = ca.localSizeLong(PEEK_ONHEAP_PRIMARY);
        backupSize = ca.localSizeLong(PEEK_ONHEAP_BACKUP);
        nearSize = ca.nearSize();
        size = primarySize + backupSize + nearSize;
        
        partitions = ca.affinity().partitions();
        near = cctx.isNear();

        metrics = new VisorCacheMetrics(ignite, name);
    }

    /**
     * @return New instance suitable to store in history.
     */
    public VisorCache history() {
        VisorCache c = new VisorCache();

        c.name = name;
        c.mode = mode;
        c.memorySize = memorySize;
        c.indexesSize = indexesSize;
        c.size = size;
        c.nearSize = nearSize;
        c.backupSize = backupSize;
        c.primarySize = primarySize;
        c.partitions = partitions;
        c.metrics = metrics;
        c.near = near;

        return c;
    }

    /**
     * @return Cache name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets new value for cache name.
     *
     * @param name New cache name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return Dynamic deployment ID.
     */
    public IgniteUuid getDynamicDeploymentId() {
        return dynamicDeploymentId;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode getMode() {
        return mode;
    }

    /**
     * @return Cache size in bytes.
     */
    public long getMemorySize() {
        return memorySize;
    }

    /**
     * @return Indexes size in bytes.
     */
    public long getIndexesSize() {
        return indexesSize;
    }

    /**
     * @return Number of all entries in cache.
     */
    public long getSize() {
        return size;
    }

    /**
     * @return Number of all entries in near cache.
     */
    public int getNearSize() {
        return nearSize;
    }

    /**
     * @return Number of backup entries in cache.
     */
    public long getBackupSize() {
        return backupSize;
    }

    /**
     * @return Number of primary entries in cache.
     */
    public long getPrimarySize() {
        return primarySize;
    }

    /**
     * @return Number of partitions.
     */
    public int getPartitions() {
        return partitions;
    }

    /**
     * @return Cache metrics.
     */
    public VisorCacheMetrics getMetrics() {
        return metrics;
    }

    /**
     * @return {@code true} if cache has near cache.
     */
    public boolean isNear() {
        return near;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        U.writeGridUuid(out, dynamicDeploymentId);
        U.writeEnum(out, mode);
        out.writeLong(memorySize);
        out.writeLong(indexesSize);
        out.writeLong(size);
        out.writeInt(nearSize);
        out.writeLong(primarySize);
        out.writeLong(backupSize);
        out.writeInt(partitions);
        out.writeBoolean(near);
        out.writeObject(metrics);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        dynamicDeploymentId = U.readGridUuid(in);
        mode = CacheMode.fromOrdinal(in.readByte());
        memorySize = in.readLong();
        indexesSize = in.readLong();
        size = in.readLong();
        nearSize = in.readInt();
        primarySize = in.readLong();
        backupSize = in.readLong();
        partitions = in.readInt();
        near = in.readBoolean();
        metrics = (VisorCacheMetrics)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCache.class, this);
    }
}
