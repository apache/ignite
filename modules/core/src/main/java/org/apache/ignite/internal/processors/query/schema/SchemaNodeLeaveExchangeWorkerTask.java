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

package org.apache.ignite.internal.processors.query.schema;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.CachePartitionExchangeWorkerTask;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Node leave exchange worker task.
 */
public class SchemaNodeLeaveExchangeWorkerTask implements CachePartitionExchangeWorkerTask {
    /** Node. */
    @GridToStringInclude
    private final ClusterNode node;

    /**
     * Constructor.
     *
     * @param node Node.
     */
    public SchemaNodeLeaveExchangeWorkerTask(ClusterNode node) {
        this.node = node;
    }

    /** {@inheritDoc} */
    @Override public boolean skipForExchangeMerge() {
        return true;
    }

    /**
     * @return Node.
     */
    public ClusterNode node() {
        return node;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaNodeLeaveExchangeWorkerTask.class, this);
    }
}
