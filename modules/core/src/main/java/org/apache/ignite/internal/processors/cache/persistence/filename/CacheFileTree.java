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

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.io.File;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheFileTree {
    /** Node file tree. */
    private final NodeFileTree ft;

    /** {@code True} if tree for metastore, {@code false} otherwise. */
    private final boolean metastore;

    /** Cache configuration. {@code Null} only if {@code metastore == true}. */
    private final @Nullable CacheConfiguration<?, ?> ccfg;

    /** Cache configuration. {@code Null} only if {@code metastore == true}. */
    private final int grpId;

    /**
     *
     * @param ft Node file tree.
     * @param metastore {@code True} if tree for metastore, {@code false} otherwise.
     * @param ccfg Cache configuration. {@code Null} only if {@code metastore == true}.
     */
    CacheFileTree(NodeFileTree ft, boolean metastore, @Nullable CacheConfiguration<?, ?> ccfg) {
        assert ccfg != null || metastore;

        this.ft = ft;
        this.metastore = metastore;
        this.ccfg = ccfg;
        this.grpId = metastore ? MetaStorage.METASTORAGE_CACHE_ID : CU.cacheGroupId(ccfg);
    }

    /**
     * @return Storage for cache.
     */
    public File storage() {
        return metastore ? ft.metaStorage() : ft.cacheStorage(ccfg);
    }

    /**
     * @return Cache configuration.
     */
    @Nullable public CacheConfiguration<?, ?> config() {
        return ccfg;
    }

    /**
     * @return {@code True} if tree for metastore, {@code false} otherwise.
     */
    public boolean metastore() {
        return metastore;
    }

    /**
     * @param part Partition id.
     * @return Partition file.
     */
    public File partition(int part) {
        return metastore
            ? ft.metaStoragePartition(part)
            : ft.partitionFile(ccfg, part);
    }

    /**
     * @return Cache name.
     */
    public String name() {
        return metastore ? MetaStorage.METASTORAGE_CACHE_NAME : ccfg.getName();
    }

    /**
     * @return Gropu id.
     */
    public int groupId() {
        return grpId;
    }
}
