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

package org.apache.ignite.util;

import org.apache.ignite.*;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.IOException;

/**
 * Test class loaders related to IGNITE-3935
 */
@GridCommonTest(group = "Utils")
public class ClassloaderSwitchSelfTest extends GridCommonAbstractTest {
    /** */
    public static class StreamingCacheEntryProcessor implements CacheEntryProcessor<String, Long, Object> {
        /** */
        @Override
        public Object process(MutableEntry<String, Long> mutableEntry, Object... objects) throws EntryProcessorException {
            Long val = mutableEntry.getValue();
            mutableEntry.setValue(val == null ? 1L : val + 1);
            return null;
        }
    }

    /**
     * Tests two grids from one process.
     */
    @Test
    public static void testClassLoading() throws IgniteException, IOException {
        IgniteConfiguration cfg = new IgniteConfiguration();
        Ignition.setClientMode(true);
        try(Ignite ignite = Ignition.start(cfg)) {
            IgniteCache<String, Long> stmCache = ignite.getOrCreateCache("mycache");
            try(IgniteDataStreamer<String, Long> strm = ignite.dataStreamer(stmCache.getName())) {
                strm.allowOverwrite(true);
                strm.receiver(StreamTransformer.from(new StreamingCacheEntryProcessor()));
                strm.addData("smth", 1L);

            }
        }
    }
}
