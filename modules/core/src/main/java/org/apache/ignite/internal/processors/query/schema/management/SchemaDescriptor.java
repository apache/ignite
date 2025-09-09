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

package org.apache.ignite.internal.processors.query.schema.management;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.processors.query.QueryTypeNameKey;

/**
 * Local database schema object.
 */
public class SchemaDescriptor {
    /** */
    private final String schemaName;

    /** */
    private final ConcurrentMap<String, TableDescriptor> tbls = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<String, ViewDescriptor> views = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<QueryTypeNameKey, TableDescriptor> typeToTbl = new ConcurrentHashMap<>();

    /** Whether schema is predefined and cannot be dorpped. */
    private final boolean predefined;

    /** Usage count. */
    private int usageCnt;

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param predefined Predefined flag.
     */
    public SchemaDescriptor(String schemaName, boolean predefined) {
        this.schemaName = schemaName;
        this.predefined = predefined;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * Increments counter for number of caches having this schema.
     */
    public void incrementUsageCount() {
        if (!predefined)
            ++usageCnt;
    }

    /**
     * Increments counter for number of caches having this schema.
     *
     * @return If schema is no longer used.
     */
    public boolean decrementUsageCount() {
        return !predefined && --usageCnt == 0;
    }

    /**
     * @return Tables.
     */
    public Collection<TableDescriptor> tables() {
        return tbls.values();
    }

    /**
     * @param tblName Table name.
     * @return Table.
     */
    public TableDescriptor tableByName(String tblName) {
        return tbls.get(tblName);
    }

    /**
     * @param typeName Type name.
     * @return Table.
     */
    public TableDescriptor tableByTypeName(String cacheName, String typeName) {
        return typeToTbl.get(new QueryTypeNameKey(cacheName, typeName));
    }

    /**
     * @param tbl Table descriptor.
     */
    public void add(TableDescriptor tbl) {
        if (tbls.putIfAbsent(tbl.type().tableName(), tbl) != null)
            throw new IllegalStateException("Table already registered: " + tbl.type().tableName());

        if (typeToTbl.putIfAbsent(new QueryTypeNameKey(tbl.cacheInfo().name(), tbl.type().name()), tbl) != null)
            throw new IllegalStateException("Table already registered: " + tbl.type().tableName());
    }

    /**
     * Drop table.
     *
     * @param tbl Table to be removed.
     */
    public void drop(TableDescriptor tbl) {
        tbls.remove(tbl.type().tableName());

        typeToTbl.remove(new QueryTypeNameKey(tbl.cacheInfo().name(), tbl.type().name()));
    }

    /**
     * @return View descriptors.
     */
    public Collection<ViewDescriptor> views() {
        return views.values();
    }

    /**
     * @param viewName View name.
     * @return View descriptor.
     */
    public ViewDescriptor viewByName(String viewName) {
        return views.get(viewName);
    }

    /**
     * @param view View descriptor.
     */
    public void add(ViewDescriptor view) {
        views.put(view.name(), view);
    }

    /**
     * Drop view.
     *
     * @param view View to be removed.
     */
    public void drop(ViewDescriptor view) {
        views.remove(view.name());
    }

    /**
     * @return {@code True} if schema is predefined.
     */
    public boolean predefined() {
        return predefined;
    }
}
