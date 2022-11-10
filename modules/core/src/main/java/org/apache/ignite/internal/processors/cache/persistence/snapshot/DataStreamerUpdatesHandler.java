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
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotHandlerType.CREATE;

/**
 * A snapshot haldler that monitors and warns of inconsistent by nature updates from DataStreamer which can issue
 * data inconsistency in snapshot.
 */
public class DataStreamerUpdatesHandler implements SnapshotHandler<Boolean> {
    /** */
    public static final String WRN_MSG = "DataStreamer with property 'allowOverwrite' set to `false` was working " +
        "during the snapshot creation. Such streaming updates are inconsistent by nature and should be successfully " +
        "finished before data usage. Snapshot might not be entirely restored. However, you would be able to restore " +
        "the caches which were not streamed into.";

    /** {@inheritDoc} */
    @Override public SnapshotHandlerType type() {
        return CREATE;
    }

    /** {@inheritDoc} */
    @Override public Boolean invoke(SnapshotHandlerContext ctx) {
        boolean streamerDetected = ctx.streamerWarning();

        if (streamerDetected)
            ctx.warning(WRN_MSG);

        return streamerDetected;
    }

    /** {@inheritDoc} */
    @Override public void complete(String name, Collection<SnapshotHandlerResult<Boolean>> results)
        throws SnapshotHandlerWarningException {
        Collection<UUID> nodes = F.viewReadOnly(results, r -> r.node().id(), SnapshotHandlerResult::data);

        if (!F.isEmpty(nodes)) {
            throw new SnapshotHandlerWarningException(WRN_MSG + " Updates from DataStreamer detected on the nodes: " +
                nodes.stream().map(UUID::toString).collect(Collectors.joining(", ")));
        }
    }
}
