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

/**
 * Configuration bean used for creating {@link MemoryPolicy} instances.
 */
public final class MemoryPolicyConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Unique name of MemoryPolicy. */
    private String name;

    /** Size in bytes of {@link PageMemory} in bytes that will be created for this configuration. */
    private long size;

    /** Path for memory mapped file (won't be created if not configured). */
    private String swapFilePath;

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
}
