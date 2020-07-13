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

package org.apache.ignite.internal.processors.cluster;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Container class to send cluster ID and tag in disco data and to write them atomically to metastorage.
 */
public class ClusterIdAndTag implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final UUID id;

    /** */
    private final String tag;

    /**
     * @param id Cluster ID.
     * @param tag Cluster tag.
     */
    public ClusterIdAndTag(UUID id, String tag) {
        this.id = id;
        this.tag = tag;
    }

    /**
     * @return Value of cluster id.
     */
    public UUID id() {
        return id;
    }

    /**
     * @return Value of cluster tag.
     */
    public String tag() {
        return tag;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(id, tag);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (!(obj instanceof ClusterIdAndTag))
            return false;

        ClusterIdAndTag idAndTag = (ClusterIdAndTag)obj;

        return id.equals(idAndTag.id) && tag.equals(idAndTag.tag);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterIdAndTag.class, this);
    }
}
