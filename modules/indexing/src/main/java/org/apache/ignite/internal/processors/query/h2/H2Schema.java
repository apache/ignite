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

import org.jsr166.ConcurrentHashMap8;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

/**
 * Database schema object.
 */
public class H2Schema {
    /** */
    private final String schemaName;

    /** */
    private final ConcurrentMap<String, H2TableDescriptor> tbls = new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<String, H2TableDescriptor> typeToTbl = new ConcurrentHashMap8<>();

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     */
    public H2Schema(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
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
     * Drop table.
     *
     * @param tbl Table to be removed.
     */
    public void drop(H2TableDescriptor tbl) {
        tbl.onDrop();

        tbls.remove(tbl.tableName());

        typeToTbl.remove(tbl.typeName());
    }

    /**
     * Called after the schema was dropped.
     */
    public void dropAll() {
        for (H2TableDescriptor tbl : tbls.values())
            tbl.onDrop();

        tbls.clear();

        typeToTbl.clear();
    }
}
