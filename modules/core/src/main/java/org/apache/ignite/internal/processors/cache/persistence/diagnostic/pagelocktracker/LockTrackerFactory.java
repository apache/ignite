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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager.MemoryCalculator;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log.LockLog;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack.LockStack;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.store.HeapPageMetaInfoStore;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.store.OffHeapPageMetaInfoStore;

import static java.lang.String.valueOf;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PAGE_LOCK_TRACKER_CAPACITY;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PAGE_LOCK_TRACKER_TYPE;
import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 * Page lock tracker factory.
 *
 * 1 - HEAP_STACK
 * 2 - HEAP_LOG
 * 3 - OFF_HEAP_STACK
 * 4 - OFF_HEAP_LOG
 */
public final class LockTrackerFactory {
    /**
     *
     */
    public static final int HEAP_STACK = 1;

    /**
     *
     */
    public static final int HEAP_LOG = 2;

    /**
     *
     */
    public static final int OFF_HEAP_STACK = 3;

    /**
     *
     */
    public static final int OFF_HEAP_LOG = 4;

    /**
     *
     */
    public static volatile int DEFAULT_CAPACITY = getInteger(IGNITE_PAGE_LOCK_TRACKER_CAPACITY, 512);

    /**
     *
     */
    public static volatile int DEFAULT_TYPE = getInteger(IGNITE_PAGE_LOCK_TRACKER_TYPE, HEAP_LOG);

    /**
     * @param name Page lock tracker name.
     */
    public static PageLockTracker<? extends PageLockDump> create(String name) {
        return create(DEFAULT_TYPE, name);
    }

    /**
     * @param name Page lock tracker name.
     * @param type Page lock tracker type.
     */
    public static PageLockTracker<? extends PageLockDump> create(int type, String name) {
        return create(type, name, DEFAULT_CAPACITY);
    }

    /**
     * @param name Page lock tracker name.
     * @param type Page lock tracker type.
     * @param size Page lock tracker size (capacity).
     */
    public static PageLockTracker<? extends PageLockDump> create(int type, String name, int size) {
        return create(type, size, name, new MemoryCalculator());
    }

    /**
     * @param name Page lock tracker name.
     * @param type Page lock tracker type.
     * @param size Page lock tracker size (capacity).
     */
    public static PageLockTracker<? extends PageLockDump> create(
        int type,
        int size,
        String name,
        MemoryCalculator memCalc
    ) {
        switch (type) {
            case HEAP_STACK:
                return new LockStack(name, new HeapPageMetaInfoStore(size, memCalc), memCalc);
            case HEAP_LOG:
                return new LockLog(name, new HeapPageMetaInfoStore(size, memCalc), memCalc);
            case OFF_HEAP_STACK:
                return new LockStack(name, new OffHeapPageMetaInfoStore(size, memCalc), memCalc);
            case OFF_HEAP_LOG:
                return new LockLog(name, new OffHeapPageMetaInfoStore(size, memCalc), memCalc);

            default:
                throw new IllegalArgumentException(valueOf(type));
        }
    }
}
