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

package org.apache.ignite.internal.processors.query.h2.affinity;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionResolver;

/**
 * Default partition resolver implementation which uses H2 to convert types appropriately.
 */
public class H2PartitionResolver implements PartitionResolver {
    /** Indexing. */
    private final IgniteH2Indexing idx;

    /**
     * Constructor.
     *
     * @param idx Indexing.
     */
    public H2PartitionResolver(IgniteH2Indexing idx) {
        this.idx = idx;
    }

    /** {@inheritDoc} */
    @Override public int partition(Object arg, int dataType, String cacheName) throws IgniteCheckedException {
        Object param = H2Utils.convert(arg, idx, dataType);

        return idx.kernalContext().affinity().partition(cacheName, param);
    }
}
