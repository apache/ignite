/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.dto;

import java.util.Objects;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DTO for node configuration.
 */
public class NodeConfiguration {
    /** Consistence id. */
    private String consistentId;

    /** Config. */
    private String json;

    /**
     * Default constructor.
     */
    public NodeConfiguration() {
        // No-op
    }

    /**
     * @param consistentId Node consistent id.
     * @param json Config in JSON format.
     */
    public NodeConfiguration(String consistentId, String json) {
        this.consistentId = consistentId;
        this.json = json;
    }

    /**
     * @return Node configuration.
     */
    public String getJson() {
        return json;
    }

    /**
     * @param json Config in JSON format.
     * @return {@code This} for chaining method calls.
     */
    public NodeConfiguration setJson(String json) {
        this.json = json;

        return this;
    }

    /**
     * @return Node consistence id.
     */
    public String getConsistentId() {
        return consistentId;
    }

    /**
     * @param consistentId Consistence id.
     * @return {@code This} for chaining method calls.
     */
    public NodeConfiguration setConsistentId(String consistentId) {
        this.consistentId = consistentId;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        NodeConfiguration that = (NodeConfiguration) o;

        return consistentId.equals(that.consistentId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(consistentId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NodeConfiguration.class, this);
    }
}
