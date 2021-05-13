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

package org.apache.ignite.internal.affinity.event;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.manager.EventParameters;
import org.apache.ignite.network.ClusterNode;

/**
 * Affinity event parameters. There are properties which associate with a concrete affinity assignment.
 */
public class AffinityEventParameters implements EventParameters {
    /** Table identifier. */
    private final UUID tableId;

    /** Table schema view. */
    private final List<List<ClusterNode>> assignment;

    /**
     * @param tableId Table identifier.
     * @param assignment Affinity assignment.
     */
    public AffinityEventParameters(
        UUID tableId,
        List<List<ClusterNode>> assignment
    ) {
        this.tableId = tableId;
        this.assignment = assignment;
    }

    /**
     * Get the table identifier.
     *
     * @return Table id.
     */
    public UUID tableId() {
        return tableId;
    }

    /**
     * Gets an affinity assignment.
     *
     * @return Affinity assignment.
     */
    public List<List<ClusterNode>> assignment() {
        return assignment;
    }
}
