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

package org.apache.ignite.internal.commandline.cache.check_indexes_inline_size;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Represents information about secondary indexes inline size from the cluster nodes.
 */
@GridInternal
public class CheckIndexInlineSizesResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Index info (index name, inline size) per node. */
    private Map<UUID, Map<String, Integer>> nodeToIndexes = new HashMap<>();

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(nodeToIndexes.size());

        for (UUID id : nodeToIndexes.keySet()) {
            U.writeUuid(out, id);

            U.writeMap(out, nodeToIndexes.get(id));
        }
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(
        byte protoVer,
        ObjectInput in
    ) throws IOException, ClassNotFoundException {
        int size = in.readInt();

        for (int i = 0; i < size; i++) {
            UUID id = U.readUuid(in);

            Map<String, Integer> map = U.readMap(in);

            nodeToIndexes.put(id, map);
        }
    }

    /**
     * Adds to result information about indexes from node.
     *
     * @param nodeId Node id.
     * @param indexNameToInlineSize Information about secondary indexes inline size.
     */
    public void addResult(UUID nodeId, Map<String, Integer> indexNameToInlineSize) {
        Map<String, Integer> prev = nodeToIndexes.put(nodeId, indexNameToInlineSize);

        assert prev == null : nodeId + " prev: " + prev + " cur: " + indexNameToInlineSize;
    }

    /**
     * Merge current result with given instance.
     *
     * @param res Given result instance.
     */
    public void merge(CheckIndexInlineSizesResult res) {
        for (Map.Entry<UUID, Map<String, Integer>> entry : res.nodeToIndexes.entrySet())
            addResult(entry.getKey(), entry.getValue());
    }

    /**
     * @return Information about secondary indexes inline size from the cluster nodes. (nodeId -> (index name, inline size)).
     */
    public Map<UUID, Map<String, Integer>> inlineSizes() {
        return nodeToIndexes;
    }
}
