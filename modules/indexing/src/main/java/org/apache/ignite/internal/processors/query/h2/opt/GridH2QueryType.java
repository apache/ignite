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

package org.apache.ignite.internal.processors.query.h2.opt;

/**
 * Query type.
 */
public enum GridH2QueryType {
    /**
     * Map query. Runs over local partitions, possibly with distributed joins.
     */
    MAP,

    /**
     * Reduce query. Local query on a node which initiated the original query.
     */
    REDUCE,

    /**
     * Local query. It may be also a query over replicated cache but all the data is available locally.
     */
    LOCAL,

    /**
     * Replicated query over a network. Such a query can be sent from a client node or node which
     * did not load all the partitions yet.
     */
    REPLICATED,

    /**
     * Parsing and optimization stage.
     */
    PREPARE,
}
