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

package org.apache.ignite.internal.sql.optimizer.affinity;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Single table with affinity info.
 */
public class PartitionTable {
    /** Alias used in the query. */
    private final String alias;

    /** Cache name. */
    private final String cacheName;

    /** Affinity column name (if can be resolved). */
    private final String affColName;

    /** Second affinity column name (possible when _KEY is affinity column and an alias for this column exists. */
    private final String secondAffColName;

    /** Join group index. */
    private int joinGrp;

    /**
     * Constructor.
     *
     * @param alias Unique alias.
     * @param cacheName Cache name.
     * @param affColName Affinity column name.
     * @param secondAffColName Second affinity column name.
     */
    public PartitionTable(
        String alias,
        String cacheName,
        @Nullable String affColName,
        @Nullable String secondAffColName
    ) {
        this.alias = alias;
        this.cacheName = cacheName;

        if (affColName == null && secondAffColName != null) {
            this.affColName = secondAffColName;
            this.secondAffColName = null;
        }
        else {
            this.affColName = affColName;
            this.secondAffColName = secondAffColName;
        }
    }

    /**
     * @return Alias.
     */
    public String alias() {
        return alias;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * Check whether passed column is affinity column.
     *
     * @param colName Column name.
     * @return {@code True} if affinity column.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isAffinityColumn(String colName) {
        return F.eq(colName, affColName) || F.eq(colName, secondAffColName);
    }

    /**
     * @return Join group index.
     */
    public int joinGroup() {
        return joinGrp;
    }

    /**
     * @param joinGrp Join group index.
     */
    public void joinGroup(int joinGrp) {
        this.joinGrp = joinGrp;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionTable.class, this);
    }
}
