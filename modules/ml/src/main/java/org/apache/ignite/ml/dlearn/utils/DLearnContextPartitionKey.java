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

package org.apache.ignite.ml.dlearn.utils;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Key used to identify d-learn partition objects.
 */
public class DLearnContextPartitionKey implements Serializable {
    /** */
    private static final long serialVersionUID = 9005844909381326835L;

    /** Index of partition. */
    private final int part;

    /** Id of learning context. */
    private final UUID learningCtxId;

    /** Key of the object. */
    private final String key;

    /**
     * Constructs a new instance of learning context partition key.
     *
     * @param part partition index
     * @param learningCtxId learning context id
     * @param key key
     */
    public DLearnContextPartitionKey(int part, UUID learningCtxId, String key) {
        this.part = part;
        this.learningCtxId = learningCtxId;
        this.key = key;
    }

    /** */
    public int getPart() {
        return part;
    }

    /** */
    public UUID getLearningCtxId() {
        return learningCtxId;
    }

    /** */
    public String getKey() {
        return key;
    }

    /** */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DLearnContextPartitionKey key1 = (DLearnContextPartitionKey)o;
        return part == key1.part &&
            Objects.equals(learningCtxId, key1.learningCtxId) &&
            Objects.equals(key, key1.key);
    }

    /** */
    @Override public int hashCode() {
        return Objects.hash(part, learningCtxId, key);
    }

    /** */
    @Override public String toString() {
        return "DLearnContextPartitionKey{" +
            "part=" + part +
            ", learningCtxId=" + learningCtxId +
            ", key='" + key + '\'' +
            '}';
    }
}
