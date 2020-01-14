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

package org.apache.ignite.internal.sql.optimizer.affinity;

import java.util.Collection;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Node denoting all available partitions
 */
public class PartitionAllNode implements PartitionNode {
    /** Singleton. */
    public static final PartitionAllNode INSTANCE = new PartitionAllNode();

    /**
     * Constructor.
     */
    private PartitionAllNode() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> apply(PartitionClientContext cliCtx, Object... args) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public int joinGroup() {
        return PartitionTableModel.GRP_NONE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionAllNode.class, this);
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return null;
    }
}
