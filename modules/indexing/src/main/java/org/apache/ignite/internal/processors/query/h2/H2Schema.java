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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.h2.mvstore.cache.CacheLongKeyLIRS;
import org.jsr166.ConcurrentHashMap8;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

/**
 * Database schema object.
 */
public class H2Schema {
    /** */
    private final String cacheName;

    /** */
    private final String schemaName;

    /** */
    private final GridUnsafeMemory offheap = null;

    /** */
    private final ConcurrentMap<String, H2TableDescriptor> tbls = new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<String, H2TableDescriptor> typeToTbl = new ConcurrentHashMap8<>();

    /** Cache for deserialized offheap rows. */
    private final CacheLongKeyLIRS<GridH2Row> rowCache;

    /** */
    private final GridCacheContext<?, ?> cctx;

    /**
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     * @param cctx Cache context.
     */
    public H2Schema(String cacheName, String schemaName, GridCacheContext<?, ?> cctx) {
        this.cacheName = cacheName;
        this.cctx = cctx;
        this.schemaName = schemaName;

        rowCache = null;
    }

    /**
     * @return Cache context.
     */
    public GridCacheContext cacheContext() {
        return cctx;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Unsafe memory.
     */
    public GridUnsafeMemory offheap() {
        return offheap;
    }

    /**
     * @return Row cache.
     */
    public CacheLongKeyLIRS<GridH2Row> rowCache() {
        return rowCache;
    }

    /**
     * @return Tables.
     */
    public Collection<H2TableDescriptor> tables() {
        return tbls.values();
    }

    /**
     * @param tblName Table name.
     * @return Table.
     */
    public H2TableDescriptor tableByName(String tblName) {
        return tbls.get(tblName);
    }

    /**
     * @param typeName Type name.
     * @return Table.
     */
    public H2TableDescriptor tableByTypeName(String typeName) {
        return typeToTbl.get(typeName);
    }

    /**
     * @param tbl Table descriptor.
     */
    public void add(H2TableDescriptor tbl) {
        if (tbls.putIfAbsent(tbl.tableName(), tbl) != null)
            throw new IllegalStateException("Table already registered: " + tbl.fullTableName());

        if (typeToTbl.putIfAbsent(tbl.typeName(), tbl) != null)
            throw new IllegalStateException("Table already registered: " + tbl.fullTableName());
    }

    /**
     * @param tbl Table descriptor.
     */
    public void remove(H2TableDescriptor tbl) {
        tbls.remove(tbl.tableName());
        typeToTbl.remove(tbl.typeName());
    }

    /**
     * Called after the schema was dropped.
     */
    public void onDrop() {
        for (H2TableDescriptor tblDesc : tbls.values())
            tblDesc.onDrop();
    }
}
