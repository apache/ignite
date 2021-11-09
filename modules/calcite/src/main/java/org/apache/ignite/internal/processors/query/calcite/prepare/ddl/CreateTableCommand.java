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

package org.apache.ignite.internal.processors.query.calcite.prepare.ddl;

import java.util.List;
import org.jetbrains.annotations.Nullable;

/**
 * CREATE TABLE statement.
 */
public class CreateTableCommand implements DdlCommand {
    /**
     * Schema name upon which this statement has been issued - <b>not</b> the name of the schema where this new table will be created.
     */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Cache name upon which new cache configuration for this table must be based. */
    private String templateName;

    /** Name of new cache associated with this table. */
    private String cacheName;

    /** Name of cache key type. */
    private String keyTypeName;

    /** Name of cache value type. */
    private String valTypeName;

    /** Group to put new cache into. */
    private String cacheGrp;

    //    /** Atomicity mode for new cache. */
    //    private CacheAtomicityMode atomicityMode;
    //
    //    /** Write sync mode. */
    //    private CacheWriteSynchronizationMode writeSyncMode;

    /** Backups number for new cache. */
    private Integer backups;

    /** Quietly ignore this command if table already exists. */
    private boolean ifNotExists;

    /** Columns. */
    private List<ColumnDefinition> cols;

    /** Primary key columns. */
    private List<String> pkCols;

    /** Name of the column that represents affinity key. */
    private String affinityKey;

    /** Data region. */
    private String dataRegionName;

    /** Encrypted flag. */
    private boolean encrypted;

    /**
     * Get cache name upon which new cache configuration for this table must be based.
     */
    public String templateName() {
        return templateName;
    }

    /**
     * Set cache name upon which new cache configuration for this table must be based.
     */
    public void templateName(String templateName) {
        this.templateName = templateName;
    }

    /**
     * Get name of new cache associated with this table.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * Set name of new cache associated with this table.
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * Get name of cache key type.
     */
    public String keyTypeName() {
        return keyTypeName;
    }

    /**
     * Set name of cache key type.
     */
    public void keyTypeName(String keyTypeName) {
        this.keyTypeName = keyTypeName;
    }

    /**
     * Get name of cache value type.
     */
    public String valueTypeName() {
        return valTypeName;
    }

    /**
     * Set name of cache value type.
     */
    public void valueTypeName(String valTypeName) {
        this.valTypeName = valTypeName;
    }

    /**
     * Get group to put new cache into.
     */
    public String cacheGroup() {
        return cacheGrp;
    }

    /**
     * Set group to put new cache into.
     */
    public void cacheGroup(String cacheGrp) {
        this.cacheGrp = cacheGrp;
    }

    //    /**
    //     * @return Atomicity mode for new cache.
    //     */
    //    public CacheAtomicityMode atomicityMode() {
    //        return atomicityMode;
    //    }
    //
    //    /**
    //     * @param atomicityMode Atomicity mode for new cache.
    //     */
    //    public void atomicityMode(CacheAtomicityMode atomicityMode) {
    //        this.atomicityMode = atomicityMode;
    //    }
    //
    //    /**
    //     * @return Write sync mode for new cache.
    //     */
    //    public CacheWriteSynchronizationMode writeSynchronizationMode() {
    //        return writeSyncMode;
    //    }
    //
    //    /**
    //     * @param writeSyncMode Write sync mode for new cache.
    //     */
    //    public void writeSynchronizationMode(CacheWriteSynchronizationMode writeSyncMode) {
    //        this.writeSyncMode = writeSyncMode;
    //    }

    /**
     * Get backups number for new cache.
     */
    @Nullable
    public Integer backups() {
        return backups;
    }

    /**
     * Set backups number for new cache.
     */
    public void backups(Integer backups) {
        this.backups = backups;
    }

    /**
     * Get columns.
     */
    public List<ColumnDefinition> columns() {
        return cols;
    }

    /**
     * Set columns.
     */
    public void columns(List<ColumnDefinition> cols) {
        this.cols = cols;
    }

    /**
     * Get primary key columns.
     */
    public List<String> primaryKeyColumns() {
        return pkCols;
    }

    /**
     * Set primary key columns.
     */
    public void primaryKeyColumns(List<String> pkCols) {
        this.pkCols = pkCols;
    }

    /**
     * Get name of the column that represents affinity key.
     */
    public String affinityKey() {
        return affinityKey;
    }

    /**
     * Set name of the column that represents affinity key.
     */
    public void affinityKey(String affinityKey) {
        this.affinityKey = affinityKey;
    }

    /**
     * Get schema name upon which this statement has been issued.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * Set schema name upon which this statement has been issued.
     */
    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * Get table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * Set table name.
     */
    public void tableName(String tblName) {
        this.tblName = tblName;
    }

    /**
     * Get quietly ignore flag of this command (ignore if table already exists).
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * Set quietly ignore flag to ignore this command if table already exists.
     */
    public void ifNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    /**
     * Get data region name.
     */
    public String dataRegionName() {
        return dataRegionName;
    }

    /**
     * Set data region name.
     */
    public void dataRegionName(String dataRegionName) {
        this.dataRegionName = dataRegionName;
    }

    /**
     * Get encrypted flag.
     */
    public boolean encrypted() {
        return encrypted;
    }

    /**
     * Set encrypted flag.
     */
    public void encrypted(boolean encrypted) {
        this.encrypted = encrypted;
    }
}
