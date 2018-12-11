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

    /** Whether table is left joined. */
    private boolean leftJoined;

    /**
     * Constructor.
     *
     * @param alias Unique alias.
     * @param cacheName Cache name.
     * @param affColName Affinity column name.
     */
    public PartitionJoinTable(String alias, String cacheName, String affColName) {
        this.alias = alias;
        this.cacheName = cacheName;
        this.affColName = affColName;
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
     * @return Affinity column name.
     */
    public String affinityColName() {
        return affColName;
    }

    /**
     * @param leftJoined Whether table is left-joined.
     */
    public void leftJoined(boolean leftJoined) {
        this.leftJoined = leftJoined;
    }

    /**
     * @return Whether table is left-joined.
     */
    public boolean leftJoined() {
        return leftJoined;
    }
}
