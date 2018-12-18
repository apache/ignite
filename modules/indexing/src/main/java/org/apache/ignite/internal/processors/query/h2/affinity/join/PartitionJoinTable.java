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

package org.apache.ignite.internal.processors.query.h2.affinity.join;

import org.jetbrains.annotations.Nullable;

/**
 * Single table with affinity info.
 */
public class PartitionJoinTable {
    /** Alias used in the query. */
    private final String alias;

    /** Cache name. */
    private final String cacheName;

    /** Affinity column name (if can be resolved). */
    private final String affColName;

    /** Second affinity column name (possible when _KEY is affinity column and an alias for this column exists. */
    private final String secondAffColName;

    /** Whether this is not a classical table. */
    private final boolean nonTable;

    /**
     * Create join table for subquery.
     *
     * @param alias Alias.
     */
    public PartitionJoinTable(String alias) {
        this.alias = alias;

        cacheName = null;
        affColName = null;
        secondAffColName = null;

        nonTable = true;
    }

    /**
     * Constructor.
     *
     * @param alias Unique alias.
     * @param cacheName Cache name.
     * @param affColName Affinity column name.
     * @param secondAffColName Second affinity column name.
     */
    public PartitionJoinTable(
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

        nonTable = false;
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
     * @return {@code True} if affinity oclumn exists.
     */
    public boolean hasAffinityColumn() {
        return affColName != null;
    }

    /**
     * @return Affinity column name.
     */
    public String affinityColName() {
        return affColName;
    }

    /**
     * @return Second affinity column name.
     */
    public String secondAffinityColName() {
        return secondAffColName;
    }

    /**
     * @return Whether this is not a classical table (subquery, union, temp tables, etc).
     */
    public boolean isNonTable() {
        return nonTable;
    }
}
