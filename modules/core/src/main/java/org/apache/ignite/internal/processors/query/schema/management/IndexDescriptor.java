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

import java.util.LinkedHashMap;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;

/**
 * Local database index object.
 */
public class IndexDescriptor {
    /** */
    private final String name;

    /** */
    private final QueryIndexType type;

    /** */
    private final LinkedHashMap<String, IndexKeyDefinition> keyDefs;

    /** */
    private final boolean isPk;

    /** */
    private final boolean isAff;

    /** */
    private final int inlineSize;

    /** Index handler. */
    private final Index idx;

    /** Table descriptor. */
    private final TableDescriptor tbl;

    /** Target index descriptor for proxy index. */
    private final IndexDescriptor targetIdx;

    /** */
    public IndexDescriptor(
        TableDescriptor tbl,
        String name,
        QueryIndexType type,
        LinkedHashMap<String, IndexKeyDefinition> keyDefs,
        boolean isPk,
        boolean isAff,
        int inlineSize,
        Index idx
    ) {
        this.tbl = tbl;
        this.name = name;
        this.type = type;
        this.keyDefs = keyDefs;
        this.isPk = isPk;
        this.isAff = isAff;
        this.targetIdx = null;
        this.inlineSize = inlineSize;
        this.idx = idx;
    }

    /** Constructor for proxy index descriptor. */
    public IndexDescriptor(
        String name,
        LinkedHashMap<String, IndexKeyDefinition> keyDefs,
        IndexDescriptor targetIdx
    ) {
        this.name = name;
        this.keyDefs = keyDefs;
        isPk = false;
        isAff = false;
        this.targetIdx = targetIdx;
        tbl = targetIdx.table();
        type = targetIdx.type();
        inlineSize = targetIdx.inlineSize();
        idx = targetIdx.index();
    }

    /** */
    public String name() {
        return name;
    }

    /** */
    public QueryIndexType type() {
        return type;
    }

    /** */
    public LinkedHashMap<String, IndexKeyDefinition> keyDefinitions() {
        return keyDefs;
    }

    /** */
    public boolean isPk() {
        return isPk;
    }

    /** */
    public boolean isAffinity() {
        return isAff;
    }

    /** */
    public boolean isProxy() {
        return targetIdx != null;
    }

    /** */
    public int inlineSize() {
        return inlineSize;
    }

    /** Index handler. */
    public Index index() {
        return idx;
    }

    /** Target index descriptor for proxy index. */
    public IndexDescriptor targetIdx() {
        return targetIdx;
    }

    /** Table descriptor. */
    public TableDescriptor table() {
        return tbl;
    }
}
