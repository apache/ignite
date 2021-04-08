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
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicStampedImpl;
import org.apache.ignite.internal.processors.datastructures.GridCacheRemovable;
import org.apache.ignite.spi.systemview.view.SystemView;

import static org.apache.ignite.internal.util.IgniteUtils.toStringSafe;

/**
 * {@link IgniteAtomicStamped} representation for a {@link SystemView}.
 *
 * @see Ignite#atomicStamped(String, Object, Object, boolean)
 * @see Ignite#atomicStamped(String, AtomicConfiguration, Object, Object, boolean)
 */
public class AtomicStampedView extends AbstractDataStructureView<GridCacheAtomicStampedImpl> {
    /** @param ds Data structure instance. */
    public AtomicStampedView(GridCacheRemovable ds) {
        super((GridCacheAtomicStampedImpl)ds);
    }

    /**
     * @return Value.
     * @see IgniteAtomicStamped#get()
     */
    @Order(1)
    public String value() {
        return toStringSafe(ds.value());
    }

    /**
     * @return Stamp.
     * @see IgniteAtomicStamped#stamp()
     */
    @Order(2)
    public String stamp() {
        return toStringSafe(ds.stamp());
    }
}
