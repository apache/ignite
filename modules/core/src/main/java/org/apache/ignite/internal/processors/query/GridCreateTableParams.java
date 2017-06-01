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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Additional params for {@code CREATE TABLE} command.
 */
public class GridCreateTableParams {
    /** Cache name upon which new cache configuration for this table must be based. */
    private String tplCacheName;

    /** Backups number for new cache. */
    private int backups;

    /** Atomicity mode for new cache. */
    private CacheAtomicityMode atomicityMode;

    /**
     * @return Cache name upon which new cache configuration for this table must be based.
     */
    public String templateCacheName() {
        return tplCacheName;
    }

    /**
     * @param tplCacheName Cache name upon which new cache configuration for this table must be based.
     * @return {@code this} for chaining.
     */
    public GridCreateTableParams templateCacheName(String tplCacheName) {
        A.notNullOrEmpty(tplCacheName, "tplCacheName");

        this.tplCacheName = tplCacheName;

        return this;
    }

    /**
     * @return Backups number for new cache.
     */
    public int backups() {
        return backups;
    }

    /**
     * @param backups Backups number for new cache.
     * @return {@code this} for chaining.
     */
    public GridCreateTableParams backups(int backups) {
        this.backups = backups;

        return this;
    }

    /**
     * @return Atomicity mode for new cache.
     */
    public CacheAtomicityMode atomicityMode() {
        return atomicityMode;
    }

    /**
     * @param atomicityMode Atomicity mode for new cache.
     * @return {@code this} for chaining.
     */
    public GridCreateTableParams atomicityMode(CacheAtomicityMode atomicityMode) {
        this.atomicityMode = atomicityMode;

        return this;
    }
}
