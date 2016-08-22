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

package org.apache.ignite.yardstick.cache;

import java.util.concurrent.atomic.AtomicLong;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.lang.IgniteAsyncCallback;

/**
 * Probe which calculate continuous query events.
 */
public class CacheEntryEventAsyncProbe extends CacheEntryEventProbe {
    /** */
    @Override protected CacheEntryUpdatedListener<Integer, Integer> localListener(AtomicLong cntr) {
        return new CacheEntryEventListener(cntr);
    }

    /**
     *
     */
    @IgniteAsyncCallback
    private static final class CacheEntryEventListener implements CacheEntryUpdatedListener<Integer, Integer> {
        /** */
        private AtomicLong cnt;

        /**
         * @param cnt Counter.
         */
        public CacheEntryEventListener(AtomicLong cnt) {
            this.cnt = cnt;
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> events)
        throws CacheEntryListenerException {
            int size = 0;

            for (CacheEntryEvent<? extends Integer, ? extends Integer> e : events)
                ++size;

            cnt.addAndGet(size);
        }
    }
}
