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
package org.apache.ignite.configuration;

import java.io.Serializable;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.database.MemoryPolicy;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;

/**
 * Configuration bean used for creating {@link MemoryPolicy} instances.
 */
public final class MemoryPolicyConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default MemoryPolicyConfiguration flag.
     *
     * Note that one and only one MemoryPolicy within the Ignite node must be marked as default.
     */
    private boolean dflt;

    /** Unique name of MemoryPolicy (must be null for default MemoryPolicy). */
    private String name;

    /** Size in bytes of {@link PageMemory} to be created. */
    private long size;

    /** */
    private String tmpFsPath;

    /** Algorithm for per-page eviction. If {@link DataPageEvictionMode#DISABLED} set, eviction is not performed. */
    private DataPageEvictionMode pageEvictionMode = DataPageEvictionMode.DISABLED;

    /** Allocation of new {@link DataPageIO} pages is stopped when this percentage of pages are allocated. */
    private double evictionThreshold = 0.9;

    /** Allocation of new {@link DataPageIO} pages is stopped by maintaining this amount of empty pages in
     * corresponding {@link FreeList} bucket. Pages get into the bucket through evicting all data entries one by one.
     * Higher load and contention can demand larger pool size.
     */
    private int emptyPagesPoolSize = 100;

    /**
     *
     */
    public boolean isDefault() {
        return dflt;
    }

    /**
     * @param dflt Default flag.
     */
    public void setDefault(boolean dflt) {
        this.dflt = dflt;
    }

    /**
     *
     */
    public String getName() {
        return name;
    }

    /**
     * @param name Name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     *
     */
    public long getSize() {
        return size;
    }

    /**
     * @param size Size of {@link PageMemory} in bytes.
     */
    public void setSize(long size) {
        this.size = size;
    }

    /**
     *
     */
    public String getTmpFsPath() {
        return tmpFsPath;
    }

    /**
     * @param tmpFsPath File path if memory-mapped file should be used.
     */
    public void setTmpFsPath(String tmpFsPath) {
        this.tmpFsPath = tmpFsPath;
    }

    /**
     *
     */
    public DataPageEvictionMode getPageEvictionMode() {
        return pageEvictionMode;
    }

    /**
     * @param evictionMode Eviction mode.
     */
    public void setPageEvictionMode(DataPageEvictionMode evictionMode) {
        this.pageEvictionMode = evictionMode;
    }

    /**
     *
     */
    public double getEvictionThreshold() {
        return evictionThreshold;
    }

    /**
     * @param evictionThreshold Eviction threshold.
     */
    public void setEvictionThreshold(double evictionThreshold) {
        this.evictionThreshold = evictionThreshold;
    }

    /**
     *
     */
    public int getEmptyPagesPoolSize() {
        return emptyPagesPoolSize;
    }

    /**
     * @param emptyPagesPoolSize Empty pages pool size.
     */
    public void setEmptyPagesPoolSize(int emptyPagesPoolSize) {
        this.emptyPagesPoolSize = emptyPagesPoolSize;
    }
}
