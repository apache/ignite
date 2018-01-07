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

package org.apache.ignite.ml.trainers.group;

import java.util.UUID;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;

/**
 * Class used as a key for caches on which {@link GroupTrainer} works.
 * Structurally it is a triple: (nodeLocalEntityIndex, trainingUUID, data);
 * nodeLocalEntityIndex is used to map key to node;
 * trainingUUID is id of training;
 * data is some custom data stored in this key, for example if we want to store three neural networks on one node
 * for training with training UUID == trainingUUID, we can use keys
 * (1, trainingUUID, networkIdx1), (1, trainingUUID, networkIdx2), (1, trainingUUID, networkIdx3).
 *
 * @param <K> Type of data part of this key.
 */
public class GroupTrainerCacheKey<K> {
    /**
     * Part of key for key-to-node affinity.
     */
    @AffinityKeyMapped
    private Long nodeLocEntityIdx;

    /**
     * UUID of training.
     */
    private UUID trainingUUID;

    /**
     * Data.
     */
    K data;

    /**
     * Construct instance of this class.
     *
     * @param nodeLocEntityIdx Part of key for key-to-node affinity.
     * @param data Data.
     * @param trainingUUID Training UUID.
     */
    public GroupTrainerCacheKey(long nodeLocEntityIdx, K data, UUID trainingUUID) {
        this.nodeLocEntityIdx = nodeLocEntityIdx;
        this.trainingUUID = trainingUUID;
        this.data = data;
    }

    /**
     * Construct instance of this class.
     *
     * @param nodeLocEntityIdx Part of key for key-to-node affinity.
     * @param data Data.
     * @param trainingUUID Training UUID.
     */
    public GroupTrainerCacheKey(int nodeLocEntityIdx, K data, UUID trainingUUID) {
        this((long)nodeLocEntityIdx, data, trainingUUID);
    }

    /**
     * Get part of key used for key-to-node affinity.
     *
     * @return Part of key used for key-to-node affinity.
     */
    public Long nodeLocalEntityIndex() {
        return nodeLocEntityIdx;
    }

    /**
     * Get UUID of training.
     *
     * @return UUID of training.
     */
    public UUID trainingUUID() {
        return trainingUUID;
    }

    /**
     * Get data.
     *
     * @return Data.
     */
    public K data() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        GroupTrainerCacheKey<?> key = (GroupTrainerCacheKey<?>)o;

        if (nodeLocEntityIdx != null ? !nodeLocEntityIdx.equals(key.nodeLocEntityIdx) : key.nodeLocEntityIdx != null)
            return false;
        if (trainingUUID != null ? !trainingUUID.equals(key.trainingUUID) : key.trainingUUID != null)
            return false;
        return data != null ? data.equals(key.data) : key.data == null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = nodeLocEntityIdx != null ? nodeLocEntityIdx.hashCode() : 0;
        res = 31 * res + (trainingUUID != null ? trainingUUID.hashCode() : 0);
        res = 31 * res + (data != null ? data.hashCode() : 0);
        return res;
    }
}
