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

package org.apache.ignite.ml.dataset.impl.cache.util;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.IgniteException;

/**
 * Exception which is thrown when partition is expected to be on the node but it isn't. In case of rebalancing it's not
 * guaranteed that partitions of caches even with the same affinity function will be moved synchronously. A workaround
 * used here is based on optimistic locking with checking that partitions available on the node.
 */
class PartitionNotFoundException extends IgniteException implements Serializable {
    /** */
    private static final long serialVersionUID = -8891869046312827676L;

    /** templace of an exception message.. */
    private static final String MSG_TEMPLATE = "Partition %d of %s expected to be on node %s, but it isn't";

    /**
     * Constructs a new instance of an upstream partition not found exception.
     *
     * @param cacheName cache name
     * @param nodeId node id
     * @param partIdx partition index
     */
    PartitionNotFoundException(String cacheName, UUID nodeId, int partIdx) {
        super(String.format(MSG_TEMPLATE, partIdx, cacheName, nodeId.toString()));
    }
}
