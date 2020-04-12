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

package org.apache.ignite.internal.processors.cache.binary;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for discovery message exchange, that is performed upon binary type
 * registration when using Entry Processor API.
 */
public class BinaryMetadataRegistrationEntryProcessorTest extends AbstractBinaryMetadataRegistrationTest {
    /** {@inheritDoc} */
    @Override protected void put(IgniteCache<Integer, Object> cache, Integer key, Object val) {
        cache.invoke(key, new CustomProcessor<>(val));
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-12675")
    @Override public void testMetadataRegisteredOnceForUserClass() {
        // NO-OP
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-12675")
    @Override public void testMetadataRegisteredOnceForBinarylizable() {
        // NO-OP
    }

    /** */
    private static class CustomProcessor<Key, Val> implements EntryProcessor<Key, Val, Val> {
        /** */
        private final Val val;

        /** */
        private CustomProcessor(Val val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Val process(MutableEntry<Key, Val> entry, Object... arguments)
            throws EntryProcessorException {
            entry.setValue(val);

            return null;
        }
    }
}
