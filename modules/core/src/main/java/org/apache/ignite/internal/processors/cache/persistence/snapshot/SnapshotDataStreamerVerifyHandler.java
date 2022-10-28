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

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotHandlerType.CREATE;

/**
 * Checks for dangerous streaming updates during snapshot.
 */
public class SnapshotDataStreamerVerifyHandler implements SnapshotHandler<SnapshotHandlerWarning> {
      /** {@inheritDoc} */
    @Override public SnapshotHandlerType type() {
        return CREATE;
    }

    /** {@inheritDoc} */
    @Override public SnapshotHandlerWarning invoke(SnapshotHandlerContext ctx) throws Exception {
        //TODO: implement
//        if (!ctx.streamedCaches().isEmpty())
            return createWarning(Collections.singletonList("default"), ctx.localNode().id());

//        return null;
    }

    /** */
    static SnapshotHandlerWarning createWarning(List<String> caches, UUID nodeId) {
        return new SnapshotHandlerWarning("Caches " +
            caches.stream().map(cn -> "'" + cn + "'").collect(Collectors.joining(",")) +
            " were under streaming loading from node '" + nodeId + "' with the streamer's property " +
            "'alowOverwrite' set to `false`. Such updates may break data consistency until finished. Snapshot " +
            "might not be entirely restored. However, you would be able to restore other caches from snapshot.");
    }
}
