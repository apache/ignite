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

package org.apache.ignite.internal.processors.query.h2.sql;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;

/**
 * CREATE TABLE statement.
 */
public class GridSqlCreateTable extends GridSqlStatement {
    /**
     * Schema name upon which this statement has been issued - <b>not</b> the name of the schema where this new table
     * will be created. */
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

    /** Atomicity mode for new cache. */
    private CacheAtomicityMode atomicityMode;

    /** Write sync mode. */
    private CacheWriteSynchronizationMode writeSyncMode;

    /** Backups number for new cache. */
    private int backups;

    /** Quietly ignore this command if table already exists. */
    private boolean ifNotExists;

    /** Columns. */
    private LinkedHashMap<String, GridSqlColumn> cols;

    /** Primary key columns. */
    private LinkedHashSet<String> pkCols;

    /** Name of the column that represents affinity key. */
    private String affinityKey;

    /** Extra WITH-params. */
    private List<String> params;

    /**
     * @return Cache name upon which new cache configuration for this table must be based.
     */
    public String templateName() {
        return templateName;
    }

    /**
     * @param templateName Cache name upon which new cache configuration for this table must be based.
     */
    public void templateName(String templateName) {
        this.templateName = templateName;
    }

    /**
     * @return Name of new cache associated with this table.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Name of new cache associated with this table.
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return Name of cache key type.
     */
    public String keyTypeName() {
        return keyTypeName;
    }

    /**
     * @param keyTypeName Name of cache key type.
     */
    public void keyTypeName(String keyTypeName) {
        this.keyTypeName = keyTypeName;
    }

    /**
     * @return Name of cache value type.
     */
    public String valueTypeName() {
        return valTypeName;
    }

    /**
     * @param valTypeName Name of cache value type.
     */
    public void valueTypeName(String valTypeName) {
        this.valTypeName = valTypeName;
    }

    /**
     * @return Group to put new cache into.
     */
    public String cacheGroup() {
        return cacheGrp;
    }

    /**
     * @param cacheGrp Group to put new cache into.
     */
    public void cacheGroup(String cacheGrp) {
        this.cacheGrp = cacheGrp;
    }

    /**
     * @return Atomicity mode for new cache.
     */
    public CacheAtomicityMode atomicityMode() {
        return atomicityMode;
    }

    /**
     * @param atomicityMode Atomicity mode for new cache.
     */
    public void atomicityMode(CacheAtomicityMode atomicityMode) {
        this.atomicityMode = atomicityMode;
    }

    /**
     * @return Write sync mode for new cache.
     */
    public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return writeSyncMode;
    }

    /**
     * @param writeSyncMode Write sync mode for new cache.
     */
    public void writeSynchronizationMode(CacheWriteSynchronizationMode writeSyncMode) {
        this.writeSyncMode = writeSyncMode;
    }

    /**
     * @return Backups number for new cache.
     */
    public int backups() {
        return backups;
    }

    /**
     * @param backups Backups number for new cache.
     */
    public void backups(int backups) {
        this.backups = backups;
    }

    /**
     * @return Columns.
     */
    public LinkedHashMap<String, GridSqlColumn> columns() {
        return cols;
    }

    /**
     * @param cols Columns.
     */
    public void columns(LinkedHashMap<String, GridSqlColumn> cols) {
        this.cols = cols;
    }

    /**
     * @return Primary key columns.
     */
    public LinkedHashSet<String> primaryKeyColumns() {
        return pkCols;
    }

    /**
     * @param pkCols Primary key columns.
     */
    public void primaryKeyColumns(LinkedHashSet<String> pkCols) {
        this.pkCols = pkCols;
    }

    /**
     * @return Name of the column that represents affinity key.
     */
    public String affinityKey() {
        return affinityKey;
    }

    /**
     * @param affinityKey Name of the column that represents affinity key.
     */
    public void affinityKey(String affinityKey) {
        this.affinityKey = affinityKey;
    }

    /**
     * @return Schema name upon which this statement has been issued.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @param schemaName Schema name upon which this statement has been issued.
     */
    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @param tblName Table name.
     */
    public void tableName(String tblName) {
        this.tblName = tblName;
    }

    /**
     * @return Quietly ignore this command if table already exists.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * @param ifNotExists Quietly ignore this command if table already exists.
     */
    public void ifNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    /**
     * @return Extra WITH-params.
     */
    public List<String> params() {
        return params;
    }

    /**
     * @param params Extra WITH-params.
     */
    public void params(List<String> params) {
        this.params = params;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return null;
    }
}
