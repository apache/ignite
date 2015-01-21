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

package org.gridgain.grid.kernal.processors.cache.local;

import org.apache.ignite.configuration.*;
import org.apache.log4j.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Tests for local transactions.
 */
public class GridCacheLocalTxSingleThreadedSelfTest extends IgniteTxSingleThreadedAbstractTest {
    /** Cache debug flag. */
    private static final boolean CACHE_DEBUG = false;

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionsConfiguration().setTxSerializableEnabled(true);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(LOCAL);

        cc.setEvictionPolicy(null);

        c.setCacheConfiguration(cc);

        // Disable log4j debug by default.
        Logger log4j = Logger.getLogger(GridCacheProcessor.class.getPackage().getName());

        log4j.setLevel(CACHE_DEBUG ? Level.DEBUG : Level.INFO);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected int keyCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int maxKeyValue() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int iterations() {
        return 20;
    }

    /** {@inheritDoc} */
    @Override protected boolean isTestDebug() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean printMemoryStats() {
        return true;
    }
}
