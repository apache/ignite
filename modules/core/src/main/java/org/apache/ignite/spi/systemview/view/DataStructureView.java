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

package org.apache.ignite.spi.systemview.view;

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKey;
import org.apache.ignite.internal.processors.datastructures.GridCacheRemovable;

/**
 * Data structure for a {@link SystemView}.
 *
 * @see Ignite#atomicLong(String, long, boolean)
 * @see Ignite#atomicLong(String, AtomicConfiguration, long, boolean)
 * @see Ignite#atomicReference(String, Object, boolean)
 * @see Ignite#atomicReference(String, AtomicConfiguration, Object, boolean)
 * @see Ignite#atomicSequence(String, long, boolean)
 * @see Ignite#atomicSequence(String, AtomicConfiguration, long, boolean)
 * @see Ignite#atomicStamped(String, Object, Object, boolean)
 * @see Ignite#atomicStamped(String, AtomicConfiguration, Object, Object, boolean)
 * @see Ignite#countDownLatch(String, int, boolean, boolean)
 * @see Ignite#semaphore(String, int, boolean, boolean)
 * @see Ignite#reentrantLock(String, boolean, boolean, boolean)
 */
public class DataStructureView {
    /**
     * @param entry
     */
    public DataStructureView(Map.Entry<GridCacheInternalKey, GridCacheRemovable> entry) {

    }

    public String name() {

    }

    public String type() {

    }

    public String groupName() {

    }

    public int groupId() {

    }

    public String options() {

    }
}
