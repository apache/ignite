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

package org.apache.ignite.internal.benchmarks.jmh.thin;

import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.cache.JmhCacheAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.profile.JmhNodeStopHook;
import org.apache.ignite.internal.benchmarks.model.SqlValueType;
import org.openjdk.jmh.annotations.Param;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/** Base of the thin client and JDBC thin allocation benchmarks: two server nodes, no client node, a table of values. */
public abstract class JmhThinClientAllocationAbstractBenchmark extends JmhCacheAbstractBenchmark {
    /** Whether the client sends a key's request to the key's primary server. */
    @Param("true")
    protected boolean partitionAwareness;

    /** Value class, which sets the table's columns. */
    @Param("POJO")
    protected SqlValueType valType;

    /** Keys the setup writes. */
    protected Integer[] keys;

    /** Values of {@link #keys}. */
    protected Object[] vals;

    /** Client connection, closed before the nodes stop. */
    private AutoCloseable conn;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        keys = new Integer[CNT];
        vals = new Object[CNT];

        for (int i = 0; i < CNT; i++) {
            keys[i] = i;
            vals[i] = valType.value(i);
        }

        populate(keys, vals);

        conn = connect();
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        // Profilers read the last iteration while the nodes still run.
        JmhNodeStopHook.run();

        conn.close();

        Ignition.stopAll(true);
    }

    /** @return Client connection to the servers' client connectors. */
    protected abstract AutoCloseable connect() throws Exception;

    /** @return Both servers' client connectors with partition awareness, otherwise the first one's, so runs agree. */
    protected String[] addresses() {
        return partitionAwareness
            ? new String[] {"127.0.0.1:10800", "127.0.0.1:10801"}
            : new String[] {"127.0.0.1:10800"};
    }

    /** {@inheritDoc} */
    @Override protected int dataNodes() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected boolean clientMode() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        return super.cacheConfiguration()
            .setAtomicityMode(ATOMIC)
            .setWriteSynchronizationMode(PRIMARY_SYNC)
            .setBackups(1)
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, valType.valueClass());
    }
}
