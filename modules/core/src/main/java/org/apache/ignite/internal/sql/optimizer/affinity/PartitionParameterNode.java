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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Node with partition which should be extracted from argument.
 */
public class PartitionParameterNode extends PartitionSingleNode {
    /** Indexing. */
    @GridToStringExclude
    private final PartitionResolver partResolver;

    /** Index. */
    private final int idx;

    /** Data type. */
    private final int dataType;

    /**
     * Constructor.
     *
     * @param tbl Table descriptor.
     * @param partResolver Partition resolver.
     * @param idx Parameter index.
     * @param dataType Parameter data type.
     */
    public PartitionParameterNode(PartitionTable tbl, PartitionResolver partResolver, int idx, int dataType) {
        super(tbl);

        this.partResolver = partResolver;
        this.idx = idx;
        this.dataType = dataType;
    }

    /** {@inheritDoc} */
    @Override public int applySingle(Object... args) throws IgniteCheckedException {
        assert args != null;
        assert idx < args.length;

        return partResolver.partition(
            args[idx],
            dataType,
            tbl.cacheName()
        );
    }

    /** {@inheritDoc} */
    @Override public boolean constant() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int value() {
        return idx;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionParameterNode.class, this);
    }
}
