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

package org.apache.ignite.internal;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests node stop while it is being accessed from EntryProcessor.
 */
public class IgniteConcurrentEntryProcessorAccessStopTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /**
     * Tests concurrent instance shutdown.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentAccess() throws Exception {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        Ignite ignite = grid();

        final IgniteCache<Object, Object> dfltCache = ignite.getOrCreateCache(ccfg);

        dfltCache.put("1", "1");

        Thread invoker = new Thread(new Runnable() {
            @Override public void run() {
                dfltCache.invoke("1", new EntryProcessor<Object, Object, Object>() {
                    @Override public Object process(MutableEntry<Object, Object> e, Object... args) {
                        int i = 100_000;

                        while (i-- >= 0)
                            grid().cluster().nodes();

                        e.remove();

                        return null;
                    }
                });
            }
        });

        invoker.setName("ConcurrentEntryProcessorActionThread");

        invoker.start();

        stopGrid();

        invoker.join();
    }
}
