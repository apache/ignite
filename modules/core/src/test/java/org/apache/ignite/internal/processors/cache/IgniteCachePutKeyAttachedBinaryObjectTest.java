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

import javax.cache.Cache.Entry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteCachePutKeyAttachedBinaryObjectTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "test";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setCacheConfiguration(new CacheConfiguration<>(CACHE_NAME))
            // For brevity, not needed to reproduce the issue.
            .setBinaryConfiguration(new BinaryConfiguration().setNameMapper(new BinaryBasicNameMapper(true)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAttachedBinaryKeyStoredSuccessfullyToNotEmptyCache() throws Exception {
        startGrid(0);

        IgniteCache<Object, Object> binCache = grid(0).cache(CACHE_NAME);

        //Ensure that cache not empty.
        AttachedKey ordinaryKey = new AttachedKey(0);

        binCache.put(ordinaryKey, 1);

        BinaryObjectBuilder holdBuilder = grid(0).binary().builder(HolderKey.class.getName());

        //Creating attached key which stores as byte array.
        BinaryObjectImpl attachedKey = holdBuilder.setField("id", new AttachedKey(1))
            .build()
            .field("id");

        //Put data with attached key.
        binCache.put(attachedKey, 2);

        assertEquals(1, binCache.get(ordinaryKey));
        assertEquals(2, binCache.get(attachedKey));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testKeyRefersToSubkeyInValue() throws Exception {
        startGrid(0);

        IgniteCache<CompositeKey, CompositeValue> cache = grid(0).cache(CACHE_NAME);

        SubKey subKey = new SubKey(10);
        CompositeKey compositeKey = new CompositeKey(20, subKey);
        CompositeValue compositeVal = new CompositeValue("foo", subKey, compositeKey);

        cache.put(compositeKey, compositeVal);

        IgniteCache<Object, Object> binCache = cache.withKeepBinary();

        BinaryObject binObj = (BinaryObject)binCache.get(compositeKey);

        binCache.put(binObj.field("key"), binObj.toBuilder().setField("val", "bar").build());

        // For debugging purpose only, may be removed after test is fixed.
        for (Entry e : binCache)
            log.info("Entry: " + e);

        assertEquals("bar", cache.get(compositeKey).val());

        assertEquals(1, cache.size());
    }

    /** */
    public static class AttachedKey {
        /** */
        public int id;

        /** */
        public AttachedKey(int id) {
            this.id = id;
        }
    }

    /** */
    public static class HolderKey {
        /** */
        public AttachedKey key;
    }

    /** */
    public static class SubKey {
        /** */
        private int subId;

        /** */
        public SubKey(int subId) {
            this.subId = subId;
        }
    }

    public static class CompositeKey {
        /** */
        private int id;

        /** */
        private SubKey subKey;

        /** */
        public CompositeKey(int id, SubKey subKey) {
            this.id = id;
            this.subKey = subKey;
        }
    }


    /** */
    public static class CompositeValue {
        /** */
        private String val;

        /** */
        private SubKey subKey;

        /** */
        private CompositeKey key;

        /** */
        public CompositeValue(String val, SubKey subKey, CompositeKey key) {
            this.val = val;
            this.subKey = subKey;
            this.key = key;
        }

        /** */
        public String val() {
            return val;
        }
    }
}
