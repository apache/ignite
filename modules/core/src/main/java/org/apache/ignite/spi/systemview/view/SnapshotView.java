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

package org.apache.ignite.spi.systemview.view;

import java.util.Set;
import org.apache.ignite.internal.managers.systemview.walker.Filtrable;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot representation for a {@link SystemView}.
 */
public class SnapshotView {
    /** Snapshot name. */
    private final String name;

    /** Node consistent id. */
    private final String consistentId;

    /** Cache group name. */
    private final String cacheGrp;

    /** Cache group local partitions. */
    private final String locPartitions;

    /**
     * @param name Snapshot name.
     * @param consistentId Node consistent id.
     * @param cacheGrp Cache group.
     * @param locPartitions Cache group local partitions.
     */
    public SnapshotView(String name, String consistentId, String cacheGrp, @Nullable Set<Integer> locPartitions) {
        this.name = name;
        this.consistentId = consistentId;
        this.cacheGrp = cacheGrp;
        this.locPartitions = locPartitions != null ? String.valueOf(locPartitions) : "[]";
    }

    /**
     * @return Snapshot name.
     */
    @Order
    @Filtrable
    public String snapshotName() {
        return name;
    }

    /**
     * @return Node consistent id.
     */
    @Order(1)
    @Filtrable
    public String consistentId() {
        return consistentId;
    }

    /**
     * @return Cache group name.
     */
    @Order(2)
    public String cacheGroup() {
        return cacheGrp;
    }

    /**
     * @return Numbers of local partitions for cache group.
     */
    @Order(3)
    public String localPartitions() {
        return locPartitions;
    }
}
