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

public class GroupTrainerCacheKey<K> {
    @AffinityKeyMapped
    private Integer nodeLocalEntityIndex;

    private UUID trainingUUID;

    K data;

    public GroupTrainerCacheKey(Integer nodeLocalEntityIndex, K data, UUID trainingUUID) {
        this.nodeLocalEntityIndex = nodeLocalEntityIndex;
        this.trainingUUID = trainingUUID;
        this.data = data;
    }

    public Integer nodeLocalEntityIndex() {
        return nodeLocalEntityIndex;
    }

    public UUID trainingUUID() {
        return trainingUUID;
    }

    public K data() {
        return data;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        GroupTrainerCacheKey<?> key = (GroupTrainerCacheKey<?>)o;

        if (nodeLocalEntityIndex != null ? !nodeLocalEntityIndex.equals(key.nodeLocalEntityIndex) : key.nodeLocalEntityIndex != null)
            return false;
        if (trainingUUID != null ? !trainingUUID.equals(key.trainingUUID) : key.trainingUUID != null)
            return false;
        return data != null ? data.equals(key.data) : key.data == null;
    }

    @Override public int hashCode() {
        int result = nodeLocalEntityIndex != null ? nodeLocalEntityIndex.hashCode() : 0;
        result = 31 * result + (trainingUUID != null ? trainingUUID.hashCode() : 0);
        result = 31 * result + (data != null ? data.hashCode() : 0);
        return result;
    }
}
