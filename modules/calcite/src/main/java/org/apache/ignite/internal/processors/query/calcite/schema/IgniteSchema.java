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

package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/**
 * Ignite schema.
 */
public class IgniteSchema extends AbstractSchema {
    /** */
    private final String schemaName;

    /** */
    private final Map<String, IgniteTable> tblMap = new ConcurrentHashMap<>();

    /** */
    private final Multimap<String, Function> funcMap = Multimaps.synchronizedMultimap(HashMultimap.create());

    /**
     * Creates a Schema.
     *
     * @param schemaName Schema name.
     */
    public IgniteSchema(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return Schema name.
     */
    public String getName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override protected Map<String, Table> getTableMap() {
        return Collections.unmodifiableMap(tblMap);
    }

    /** {@inheritDoc} */
    @Override protected Multimap<String, Function> getFunctionMultimap() {
        return Multimaps.unmodifiableMultimap(funcMap);
    }

    /**
     * @param tbl Table.
     */
    public void addTable(String tblName, IgniteTable tbl) {
        tblMap.put(tblName, tbl);
    }

    /**
     * @param tblName Table name.
     */
    public void removeTable(String tblName) {
        tblMap.remove(tblName);
    }

    /**
     * @param name Function name.
     * @param func SQL function.
     */
    public void addFunction(String name, Function func) {
        funcMap.put(name, func);
    }
}
