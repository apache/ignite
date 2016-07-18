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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteCacheInternalEventsIgnoreSelfTest extends GridCommonAbstractTest {
    /** */
    private static final AtomicBoolean evtFlag = new AtomicBoolean();

    /**
     * @throws Exception if failed.
     */
    public void testInternalEventsIgnore() throws Exception {
        Ignite ignite = startGrid(1);
        ignite.events().localListen(new EventListener(), EventType.EVT_TASK_STARTED, EventType.EVT_TASK_REDUCED,
            EventType.EVT_TASK_FINISHED);
        IgniteCache cache = ignite.createCache(defaultCacheConfiguration().setName("myTestCache"));
        cache.size(CachePeekMode.ALL);
        cache.sizeLong(CachePeekMode.ALL);

        assertFalse(evtFlag.get());
    }

    /**
     *
     */
    private static final class EventListener implements IgnitePredicate<Event> {
        /** {@inheritDoc} */
        @Override public boolean apply(Event e) {
            evtFlag.set(true);

            return true;
        }
    }
}
