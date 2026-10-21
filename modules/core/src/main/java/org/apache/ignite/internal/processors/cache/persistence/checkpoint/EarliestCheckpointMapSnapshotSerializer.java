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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObjectSerializer;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.EarliestCheckpointMapSnapshot.GroupStateSnapshot;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Serializer that was generated for {@link EarliestCheckpointMapSnapshot} while it extended
 * {@code IgniteDataTransferObject}, i.e. by builds with IGNITE-27775 and without IGNITE-29185.
 * It writes UUIDs with {@link U#writeUuid} while the legacy layout (2.17, 2.18, current) uses
 * {@link ObjectOutput#writeObject}, so {@code cpMapSnapshot.bin} written by such builds can't be read by
 * {@link EarliestCheckpointMapSnapshot#readExternal} directly. Kept as is, to read such files on upgrade.
 *
 * <b>The class is a copy of the generated code and must not be changed.</b>
 */
public class EarliestCheckpointMapSnapshotSerializer implements IgniteDataTransferObjectSerializer<EarliestCheckpointMapSnapshot> {
    /** {@inheritDoc} */
    @Override public void writeExternal(EarliestCheckpointMapSnapshot obj, ObjectOutput out) throws IOException {
        {
            int len = obj.data == null ? -1 : obj.data.size();
            out.writeInt(len);
            if (len > 0) {
                for (Map.Entry<UUID, Map<java.lang.Integer, org.apache.ignite.internal.processors.cache.persistence.checkpoint.EarliestCheckpointMapSnapshot.GroupStateSnapshot>> el : obj.data.entrySet()) {
                    UUID k = el.getKey();
                    Map<java.lang.Integer, org.apache.ignite.internal.processors.cache.persistence.checkpoint.EarliestCheckpointMapSnapshot.GroupStateSnapshot> v = el.getValue();
                    U.writeUuid(out, k);
                    {
                        int len1 = v == null ? -1 : v.size();
                        out.writeInt(len1);
                        if (len1 > 0) {
                            for (Map.Entry<Integer, GroupStateSnapshot> el1 : v.entrySet()) {
                                Integer k1 = el1.getKey();
                                GroupStateSnapshot v1 = el1.getValue();
                                out.writeObject(k1);
                                out.writeObject(v1);
                            }
                        }
                    }
                }
            }
        }
        {
            int len = obj.checkpointIds == null ? -1 : obj.checkpointIds.size();
            out.writeInt(len);
            if (len > 0) {
                for (java.util.UUID el : obj.checkpointIds) {
                    U.writeUuid(out, el);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void readExternal(EarliestCheckpointMapSnapshot obj, ObjectInput in) throws IOException, ClassNotFoundException {
        {
            int len = in.readInt();
            if (len >= 0) {
                obj.data = new HashMap<>();
                for (int i = 0; i < len; i++) {
                    UUID k = null;
                    Map<java.lang.Integer, org.apache.ignite.internal.processors.cache.persistence.checkpoint.EarliestCheckpointMapSnapshot.GroupStateSnapshot> v = null;
                    k = U.readUuid(in);
                    {
                        int len1 = in.readInt();
                        if (len1 >= 0) {
                            v = new HashMap<>();
                            for (int i1 = 0; i1 < len1; i1++) {
                                Integer k1 = null;
                                GroupStateSnapshot v1 = null;
                                k1 = (Integer)in.readObject();
                                v1 = (GroupStateSnapshot)in.readObject();
                                v.put(k1, v1);
                            }
                        }
                    }
                    obj.data.put(k, v);
                }
            }
        }
        {
            int len = in.readInt();
            if (len >= 0) {
                obj.checkpointIds = new HashSet<>();
                for (int i = 0; i < len; i++) {
                    java.util.UUID el = null;
                    el = U.readUuid(in);
                    obj.checkpointIds.add(el);
                }
            }
        }
    }
}
