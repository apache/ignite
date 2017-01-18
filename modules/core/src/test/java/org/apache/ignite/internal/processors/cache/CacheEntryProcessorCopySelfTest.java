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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for value copy in entry processor.
 */
public class CacheEntryProcessorCopySelfTest extends GridCommonAbstractTest {
    /** Old value. */
    private static final int OLD_VAL = 100;

    /** New value. */
    private static final int NEW_VAL = 200;

    /** Empty array. */
    private static final int[] EMPTY_ARR = new int[0];

    /** Deserializations counter. */
    private static final AtomicInteger cnt = new AtomicInteger();

    /** p2p enabled. */
    private boolean p2pEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setIncludeEventTypes(EMPTY_ARR);

        cfg.setPeerClassLoadingEnabled(p2pEnabled);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMutableEntryWithP2PEnabled() throws Exception {
        doTestMutableEntry(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMutableEntryWithP2PDisabled() throws Exception {
        doTestMutableEntry(false);
    }

    /**
     *
     */
    private void doTestMutableEntry(boolean p2pEnabled) throws Exception {
        this.p2pEnabled = p2pEnabled;

        Ignite grid = startGrid();

        assertEquals(p2pEnabled, grid.configuration().isPeerClassLoadingEnabled());

        try {
            // One deserialization due to copyOnRead == true.
            doTest(true, false, OLD_VAL, 1);

            // One deserialization due to copyOnRead == true.
            // Additional deserialization in case p2p enabled and not BinaryMarshaller due to storeValue == true on update entry.
            doTest(true, true, NEW_VAL, p2pEnabled &&
                !(grid.configuration().getMarshaller() instanceof BinaryMarshaller) ? 2 : 1);

            // No deserialization.
            doTest(false, false, NEW_VAL, 0);

            // One deserialization due to storeValue == true.
            doTest(false, true, NEW_VAL, 1);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param cpOnRead Copy on read.
     * @param mutate Mutate.
     * @param expVal Expected value.
     * @param expCnt Expected deserializations count.
     */
    @SuppressWarnings("unchecked")
    private void doTest(boolean cpOnRead, final boolean mutate, int expVal, int expCnt) throws Exception {

        Ignite ignite = grid();

        CacheConfiguration ccfg = defaultCacheConfiguration();
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setCopyOnRead(cpOnRead);
        ccfg.setNearConfiguration(null);

        IgniteCache<Integer, Value> cache = null;

        try {
            cache = ignite.createCache(ccfg);

            cache.put(0, new Value(OLD_VAL));

            cache.get(0);

            cnt.set(0);

            cache.invoke(0, new CacheEntryProcessor<Integer, Value, Object>() {
                @Override public Object process(MutableEntry<Integer, Value> entry, Object... args) {
                    Value val = entry.getValue();

                    val.i = NEW_VAL;

                    if (mutate)
                        entry.setValue(val);

                    return null;
                }
            });

            CacheObject obj = ((GridCacheAdapter)((IgniteCacheProxy)cache).delegate()).peekEx(0).peekVisibleValue();

            int actCnt = cnt.get();

            if (obj instanceof BinaryObject)
                if (cpOnRead)
                    assertEquals(expVal, (int)((BinaryObject)obj).field("i"));
                else
                    assertEquals(expVal, ((Value)U.field(obj, "obj")).i);
            else {
                if (storeValue(cache))
                    assertEquals(expVal, U.<Value>field(obj, "val").i);
                else
                    assertEquals(expVal, CU.<Value>value(obj, ((IgniteCacheProxy)cache).context(), false).i);
            }

            assertEquals(expCnt, actCnt);
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /**
     * @param cache Cache.
     */
    private static boolean storeValue(IgniteCache cache) {
        return ((IgniteCacheProxy)cache).context().cacheObjectContext().storeValue();
    }

    /**
     *
     */
    private static class Value implements Externalizable {
        /**  */
        private int i;

        /**
         * Default constructor (required by Externalizable).
         */
        public Value() {
        }

        /**
         * @param i I.
         */
        public Value(int i) {
            this.i = i;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(i);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            i = in.readInt();

            cnt.incrementAndGet();
        }
    }
}
