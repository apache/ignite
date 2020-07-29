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

package org.apache.ignite.internal.ducktest.tests.smoke_test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionState;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class SimpleApplication extends IgniteAwareApplication {
    /**
     * @param ignite Ignite.
     */
    public SimpleApplication(Ignite ignite) {
        super(ignite);
    }

    /** {@inheritDoc} */
    @Override public void run(String[] args) {
        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache("CACHE");

        cache.put(1, 2);

        markInitialized();

        while (!terminated()) {
            try {
                U.sleep(100); // Keeping node/txs alive.
            }
            catch (IgniteInterruptedCheckedException ignored) {
                log.info("Waiting interrupted.");
            }
        }

        markFinished();
    }
}
