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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.Ignite;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

/**
 * Tests transaction labels.
 */
public class TxLabelTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0).getOrCreateCache(defaultCacheConfiguration());
    }

    /**
     * Tests transaction labels.
     */
    @Test
    public void testLabel() {
        testLabel0(grid(0), "lbl0");
        testLabel0(grid(0), "lbl1");

        try {
            testLabel0(grid(0), null);

            fail();
        }
        catch (Exception e) {
            // Expected.
        }
    }

    /**
     * @param ignite Ignite.
     * @param lbl Label.
     */
    private void testLabel0(Ignite ignite, String lbl) {
        try (Transaction tx = ignite.transactions().withLabel(lbl).txStart()) {
            assertEquals(lbl, tx.label());

            ignite.cache(DEFAULT_CACHE_NAME).put(0, 0);

            tx.commit();
        }
    }
}
