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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for partitions map.
 */
public class VisorPartitionMap extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Map of partition states. */
    private Map<Integer, GridDhtPartitionState> parts;

    /**
     * Default constructor.
     */
    public VisorPartitionMap() {
        // No-op.
    }

    /**
     * @param map Partitions map.
     */
    public VisorPartitionMap(GridDhtPartitionMap map) {
        parts = map.map();
    }

    /**
     * @return Partitions map.
     */
    public Map<Integer, GridDhtPartitionState> getPartitions() {
        return parts;
    }

    /**
     * @return Partitions map size.
     */
    public int size() {
        return parts.size();
    }

    /**
     * @param part Partition.
     * @return Partition state.
     */
    public GridDhtPartitionState get(Integer part) {
        return parts.get(part);
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        if (parts != null) {
            out.writeInt(parts.size());

            for (Map.Entry<Integer, GridDhtPartitionState> e : parts.entrySet()) {
                out.writeInt(e.getKey());
                U.writeEnum(out, e.getValue());
            }
        }
        else
            out.writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            parts = null;
        else {
            parts = new HashMap<>(size, 1.0f);

            for (int i = 0; i < size; i++)
                parts.put(in.readInt(), GridDhtPartitionState.fromOrdinal(in.readByte()));
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorPartitionMap.class, this);
    }
}
