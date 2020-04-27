/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectBuilder;
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
            .setCacheConfiguration(new CacheConfiguration<>(CACHE_NAME));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

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
     *
     */
    public static class AttachedKey {
        /** */
        public int id;

        /** **/
        public AttachedKey(int id) {
            this.id = id;
        }
    }

    /**
     *
     */
    public static class HolderKey {
        /** */
        public AttachedKey key;
    }
}
