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

package org.apache.ignite.client;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;

/** Affinity configuration. */
public final class ClientAffinityConfiguration implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** @serial Total number of partitions. */
    private int partitions = -1;

    /** @serial Flag to exclude same-host-neighbors from being backups of each other. */
    private boolean excludeNeighbors;

    /** Default constructor. */
    public ClientAffinityConfiguration() {
        // No-op.
    }

    /**
     * Gets total number of partitions.
     *
     * @return Total number of partitions.
     */
    public int getPartitions() {
        return partitions;
    }

    /**
     * Sets total number of partitions.
     *
     * @param partitions Total number of partitions.
     * @return {@code this} for chaining.
     */
    public ClientAffinityConfiguration setPartitions(int partitions) {
        this.partitions = partitions;

        return this;
    }

    /**
     * Checks flag to exclude same-host-neighbors from being backups of each other (default is {@code false}).
     *
     * @return {@code True} if nodes residing on the same host may not act as backups of each other.
     */
    public boolean isExcludeNeighbors() {
        return excludeNeighbors;
    }

    /**
     * Sets flag to exclude same-host-neighbors from being backups of each other (default is {@code false}).
     *
     * @param excludeNeighbors {@code True} if nodes residing on the same host may not act as backups of each other.
     * @return {@code this} for chaining.
     */
    public ClientAffinityConfiguration setExcludeNeighbors(boolean excludeNeighbors) {
        this.excludeNeighbors = excludeNeighbors;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClientAffinityConfiguration.class, this);
    }
}
