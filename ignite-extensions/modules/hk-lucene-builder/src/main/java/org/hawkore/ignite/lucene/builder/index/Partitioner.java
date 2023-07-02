/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkore.ignite.lucene.builder.index;

import org.hawkore.ignite.lucene.builder.JSONBuilder;
import org.hawkore.ignite.lucene.builder.index.Partitioner.None;
import org.hawkore.ignite.lucene.builder.index.Partitioner.OnToken;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * An index partitioner to split the index in multiple partitions.
 *
 * Index partitioning is useful to speed up some searches to the detriment of others, depending on the implementation.
 *
 * It is also useful to overcome the  Lucene's hard limit of 2147483519 documents per local index.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type", defaultImpl = None.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = None.class, name = "none"),
        @JsonSubTypes.Type(value = OnToken.class, name = "token")
        })
public abstract class Partitioner extends JSONBuilder {

    /**
     * {@link Partitioner} with no action, equivalent to not defining a partitioner.
     */
    public static class None extends Partitioner {
    }

    /**
     * A {@link Partitioner} based on the partition key token. Rows will be stored in an index partition determined by
     * the cache key. Partition-directed searches will be routed to a single partition, increasing
     * performance. However, searches with no key conditions will be routed to all the partitions, with a slightly lower
     * performance.
     *
     * This partitioner guarantees an excellent load balancing between index partitions.
     *
     * The number of partitions per node should be specified.
     */
    public static class OnToken extends Partitioner {

        /** The number of partitions per node. */
        @JsonProperty("partitions")
        public final int partitions;

        /**
         * Builds a new partitioner on token with the specified number of partitions per node.
         *
         * @param partitions the number of index partitions per node
         */
        public OnToken(int partitions) {
            this.partitions = partitions;
        }
    }
}
