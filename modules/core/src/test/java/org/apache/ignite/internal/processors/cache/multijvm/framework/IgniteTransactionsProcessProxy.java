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

package org.apache.ignite.internal.processors.cache.multijvm.framework;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.transactions.*;

import java.util.*;

/**
 * Ignite transactions proxy for ignite instance at another JVM.
 */
public class IgniteTransactionsProcessProxy implements IgniteTransactions {
    /** Compute. */
    private final transient IgniteCompute compute;

    /** Grid id. */
    private final UUID gridId;

    /**
     * @param proxy Ignite process proxy.
     */
    public IgniteTransactionsProcessProxy(IgniteProcessProxy proxy) {
        gridId = proxy.getId();

        ClusterGroup grp = proxy.localJvmGrid().cluster().forNodeId(proxy.getId());

        compute = proxy.localJvmGrid().compute(grp);
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart() throws IllegalStateException {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override
    public Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation, long timeout,
        int txSize) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public Transaction tx() {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public TransactionMetrics metrics() {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        // TODO: CODE: implement.
    }
}
