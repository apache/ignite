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

package org.apache.ignite.internal.jdbc.thin;

import org.apache.ignite.internal.sql.optimizer.affinity.PartitionClientContext;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionResult;

/**
 * Partition result descriptor.
 */
public class JdbcThinPartitionResultDescriptor {
    /** Partition result. */
    private final PartitionResult partRes;

    /** Cache id. */
    private final int cacheId;

    /** Partition client context. */
    private final PartitionClientContext partClientCtx;

    /** Stub to use as empty descriptor. */
    public static final JdbcThinPartitionResultDescriptor EMPTY_DESCRIPTOR =
        new JdbcThinPartitionResultDescriptor(null, -1, null);

    /**
     * Constructor.
     *
     * @param partRes Partiton result.
     * @param cacheId Cache id.
     * @param partClientCtx Partition client context.
     */
    public JdbcThinPartitionResultDescriptor(PartitionResult partRes, int cacheId,
        PartitionClientContext partClientCtx) {
        this.partRes = partRes;

        this.cacheId = cacheId;

        this.partClientCtx = partClientCtx;
    }

    /**
     * @return Cache Id.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @return Partition result.
     */
    public PartitionResult partitionResult() {
        return partRes;
    }

    /**
     * @return Partition client context.
     */
    public PartitionClientContext partitionClientContext() {
        return partClientCtx;
    }
}
