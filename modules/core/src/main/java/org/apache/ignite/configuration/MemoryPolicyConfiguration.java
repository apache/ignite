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
import org.apache.ignite.internal.mem.OutOfMemoryException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.database.MemoryPolicy;

/**
 * This class defines {@code MemoryPolicy} configuration.
 */
public final class MemoryPolicyConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Memory policy name. */
    private String name;

    /** Memory policy size. */
    private long size;

    /** An optional path to a memory mapped file for this memory policy. */
    private String swapFilePath;

    /** Algorithm for per-page eviction. If {@link DataPageEvictionMode#DISABLED} set, eviction is not performed. */
    private DataPageEvictionMode pageEvictionMode = DataPageEvictionMode.DISABLED;

    /** Threshold for per-page eviction.
     * When this percentage of memory pages of the current policy is allocated (90% by default),
     * system starts page eviction.
     * Decrease this parameter if {@link OutOfMemoryException} occurred with enabled page eviction.
     */
    private double evictionThreshold = 0.9;

    /** When {@link #evictionThreshold} is reached, allocation of new data pages is prevented by maintaining this
     * amount of evicted data pages in the pool. If any thread needs free page to store cache entry,
     * it will take empty page from the pool instead of allocating a new one.
     * Increase this parameter if cache can contain very big entries (total size of pages in the pool should be enough
     * to contain largest cache entry).
     * Increase this parameter if {@link OutOfMemoryException} occurred with enabled page eviction.
     */
    private int emptyPagesPoolSize = 100;

    /**
     * Unique name of MemoryPolicy.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name Unique name of MemoryPolicy.
     */
    public MemoryPolicyConfiguration setName(String name) {
        this.name = name;

        return this;
    }

    /**
     * Size in bytes of {@link PageMemory} in bytes that will be created for this configuration.
     */
    public long getSize() {
        return size;
    }

    /**
     * Size in bytes of {@link PageMemory} in bytes that will be created for this configuration.
     */
    public MemoryPolicyConfiguration setSize(long size) {
        this.size = size;

        return this;
    }

    /**
     * @return Path for memory mapped file (won't be created if not configured).
     */
    public String getSwapFilePath() {
        return swapFilePath;
    }

    /**
     * @param swapFilePath Path for memory mapped file (won't be created if not configured)..
     */
    public MemoryPolicyConfiguration setSwapFilePath(String swapFilePath) {
        this.swapFilePath = swapFilePath;

        return this;
    }

    /**
     * Gets data page eviction mode.
     */
    public DataPageEvictionMode getPageEvictionMode() {
        return pageEvictionMode;
    }

    /**
     * Sets data page eviction mode.
     *
     * @param evictionMode Eviction mode.
     */
    public MemoryPolicyConfiguration setPageEvictionMode(DataPageEvictionMode evictionMode) {
        pageEvictionMode = evictionMode;

        return this;
    }

    /**
     * Gets data page eviction threshold.
     *
     * @return Data page eviction threshold.
     */
    public double getEvictionThreshold() {
        return evictionThreshold;
    }

    /**
     * Sets data page eviction threshold.
     *
     * @param evictionThreshold Eviction threshold.
     */
    public MemoryPolicyConfiguration setEvictionThreshold(double evictionThreshold) {
        this.evictionThreshold = evictionThreshold;

        return this;
    }

    /**
     * Gets empty pages pool size.
     */
    public int getEmptyPagesPoolSize() {
        return emptyPagesPoolSize;
    }

    /**
     * Sets empty pages pool size.
     *
     * @param emptyPagesPoolSize Empty pages pool size.
     */
    public MemoryPolicyConfiguration setEmptyPagesPoolSize(int emptyPagesPoolSize) {
        this.emptyPagesPoolSize = emptyPagesPoolSize;

        return this;
    }
}
