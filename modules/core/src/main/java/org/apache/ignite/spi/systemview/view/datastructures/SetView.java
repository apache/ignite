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

package org.apache.ignite.spi.systemview.view.datastructures;

import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.datastructures.GridCacheSetProxy;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.systemview.view.SystemView;

/**
 * Set representation for a {@link SystemView}.
 */
public class SetView {
    /** Set. */
    private final GridCacheSetProxy<?> set;

    /**
     * @param set Set to view.
     */
    public SetView(GridCacheSetProxy<?> set) {
        this.set = set;
    }

    /** @return Set id. */
    @Order
    public IgniteUuid id() {
        return set.delegate().id();
    }

    /** @return Set name. */
    @Order(1)
    public String name() {
        return set.name();
    }

    /** @return Set size. */
    @Order(2)
    public int size() {
        return set.size();
    }

    /** @return Cache group name where values for set stored. */
    @Order(3)
    public String groupName() {
        return set.groupName();
    }

    /** @return Cache group id where values for set stored. */
    @Order(4)
    public int groupId() {
        return set.groupId();
    }

    /** @return Collocated flag. */
    public boolean collocated() {
        return set.collocated();
    }

    /** @return If {@code true} then this set removed. */
    public boolean removed() {
        return set.removed();
    }
}
