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
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.datastructures.GridCacheCountDownLatchImpl;
import org.apache.ignite.internal.processors.datastructures.GridCacheRemovable;
import org.apache.ignite.spi.systemview.view.SystemView;

/**
 * {@link IgniteCountDownLatch} representation for a {@link SystemView}.
 *
 * @see Ignite#countDownLatch(String, int, boolean, boolean)
 */
public class CountDownLatchView extends AbstractDataStructureView<GridCacheCountDownLatchImpl> {
    /** @param ds Data structure instance. */
    public CountDownLatchView(GridCacheRemovable ds) {
        super((GridCacheCountDownLatchImpl)ds);
    }

    /**
     * @return Count.
     * @see IgniteCountDownLatch#count()
     */
    @Order(1)
    public int count() {
        return ds.count();
    }

    /**
     * @return Initial count.
     * @see IgniteCountDownLatch#initialCount()
     */
    @Order(2)
    public int initialCount() {
        return ds.initialCount();
    }

    /**
     * @return {@code True} if latch is auto removed after counter down to 0.
     * @see IgniteCountDownLatch#autoDelete()
     */
    @Order(3)
    public boolean autoDelete() {
        return ds.autoDelete();
    }
}
