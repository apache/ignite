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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Helper class to override cache configuration.
 */
public class CacheConfigurationOverride {
    /** */
    private CacheMode mode;

    /** */
    private Integer backups;

    /** */
    private String cacheGroup;

    /** */
    private String dataRegion;

    /** */
    private CacheWriteSynchronizationMode writeSync;

    /**
     * @return Cache mode.
     */
    public CacheMode mode() {
        return mode;
    }

    /**
     * @param mode New cache mode.
     * @return {@code this} for chaining.
     */
    public CacheConfigurationOverride mode(CacheMode mode) {
        this.mode = mode;

        return this;
    }

    /**
     * @return Number of backup nodes for one partition.
     */
    public Integer backups() {
        return backups;
    }

    /**
     * @param backups New number of backup nodes for one partition.
     * @return {@code this} for chaining.
     */
    public CacheConfigurationOverride backups(Integer backups) {
        this.backups = backups;

        return this;
    }

    /**
     * @return Cache group name.
     */
    public String cacheGroup() {
        return cacheGroup;
    }

    /**
     * @param grpName New cache group name.
     * @return {@code this} for chaining.
     */
    public CacheConfigurationOverride cacheGroup(String grpName) {
        this.cacheGroup = grpName;

        return this;
    }

    /**
     * @return Data region name.
     */
    public String dataRegion() {
        return dataRegion;
    }

    /**
     * @param dataRegName Data region name.
     * @return {@code this} for chaining.
     */
    public CacheConfigurationOverride dataRegion(String dataRegName) {
        this.dataRegion = dataRegName;

        return this;
    }

    /**
     * @return Write synchronization mode.
     */
    public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return writeSync;
    }

    /**
     * @param writeSync New write synchronization mode.
     * @return {@code this} for chaining.
     */
    public CacheConfigurationOverride writeSynchronizationMode(CacheWriteSynchronizationMode writeSync) {
        this.writeSync = writeSync;

        return this;
    }

    /**
     * Apply overrides to specified cache configuration.
     *
     * @param ccfg Cache configuration to override.
     * @return Updated cache configuration to permit fluent-style method calls.
     */
    public CacheConfiguration apply(CacheConfiguration ccfg) {
        assert ccfg != null;

        if (mode != null)
            ccfg.setCacheMode(mode);

        if (backups != null)
            ccfg.setBackups(backups);

        if (cacheGroup != null)
            ccfg.setGroupName(cacheGroup);

        if (dataRegion != null)
            ccfg.setDataRegionName(dataRegion);

        if (writeSync != null)
            ccfg.setWriteSynchronizationMode(writeSync);

        return ccfg;
    }

    /**
     * @return {@code true} If nothing was set.
     */
    public boolean isEmpty() {
        return mode == null &&
            backups == null &&
            cacheGroup == null &&
            dataRegion == null &&
            writeSync == null;
    }
}
