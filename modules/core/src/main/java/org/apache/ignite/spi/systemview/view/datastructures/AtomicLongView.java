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
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicLongImpl;
import org.apache.ignite.internal.processors.datastructures.GridCacheRemovable;
import org.apache.ignite.spi.systemview.view.SystemView;

/**
 * {@link IgniteAtomicLong} representation for a {@link SystemView}.
 *
 * @see Ignite#atomicLong(String, long, boolean)
 * @see Ignite#atomicLong(String, AtomicConfiguration, long, boolean)
 */
public class AtomicLongView extends AbstractDataStructureView<GridCacheAtomicLongImpl> {
    /** @param ds Data structure instance. */
    public AtomicLongView(GridCacheRemovable ds) {
        super((GridCacheAtomicLongImpl)ds);
    }

    /**
     * @return Value.
     * @see IgniteAtomicLong#get()
     */
    @Order(1)
    public long value() {
        return ds.get();
    }
}
