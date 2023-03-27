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

package org.apache.ignite.spi.systemview.view.datastructures;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicSequenceImpl;
import org.apache.ignite.internal.processors.datastructures.GridCacheRemovable;
import org.apache.ignite.spi.systemview.view.SystemView;

/**
 * {@link IgniteAtomicSequence} representation for a {@link SystemView}.
 *
 * @see Ignite#atomicSequence(String, long, boolean)
 * @see Ignite#atomicSequence(String, AtomicConfiguration, long, boolean)
 */
public class AtomicSequenceView extends AbstractDataStructureView<GridCacheAtomicSequenceImpl> {
    /** @param ds Data structure instance. */
    public AtomicSequenceView(GridCacheRemovable ds) {
        super((GridCacheAtomicSequenceImpl)ds);
    }

    /**
     * @return Value.
     * @see IgniteAtomicSequence#get()
     */
    @Order(1)
    public long value() {
        return ds.get();
    }

    /**
     * @return Batch size.
     * @see IgniteAtomicSequence#batchSize()
     */
    @Order(2)
    public long batchSize() {
        return ds.batchSize();
    }
}
