/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

/** Result of {@link SnapshotDeleteProcess}. */
public final class SnapshotDeleteProcessResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Nodes which found snapshot data and completely removed it. */
    @Order(0)
    Collection<UUID> completedNodes;

    /** Nodes which found snapshot data but didn't remove it completely. */
    @Order(1)
    Collection<UUID> uncompletedNodes;

    /** Server nodes which didn't find any snapshot data. */
    @Order(2)
    Collection<UUID> emptyNodes;

    /** Snapshot's baseline nodes which aren't found in current cluster. */
    @Order(3)
    Collection<String> absentBaselines;

    /** Default constructor for {@link MessageFactory}. */
    public SnapshotDeleteProcessResult() {
        // No-op.
    }

    /** */
    public SnapshotDeleteProcessResult(
        Collection<UUID> completedNodes,
        Collection<UUID> uncompletedNodes,
        Collection<UUID> emptyNodes,
        Collection<String> absentBaselines
    ) {
        this.completedNodes = completedNodes;
        this.uncompletedNodes = uncompletedNodes;
        this.emptyNodes = emptyNodes;
        this.absentBaselines = absentBaselines;
    }

    /** */
    public Collection<UUID> completedNodes() {
        return Collections.unmodifiableCollection(completedNodes);
    }

    /** */
    public Collection<UUID> uncompletedNodes() {
        return Collections.unmodifiableCollection(uncompletedNodes);
    }

    /** */
    public Collection<UUID> emptyNodes() {
        return Collections.unmodifiableCollection(emptyNodes);
    }

    /** */
    public Collection<String> absentBaselines() {
        return Collections.unmodifiableCollection(absentBaselines);
    }
}
