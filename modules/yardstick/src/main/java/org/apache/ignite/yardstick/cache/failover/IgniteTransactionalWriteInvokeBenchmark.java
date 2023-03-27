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

package org.apache.ignite.yardstick.cache.failover;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.util.typedef.F;
import org.yardstickframework.BenchmarkConfiguration;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.yardstick.IgniteBenchmarkUtils.doInTransaction;
import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Transactional write invoke failover benchmark.
 * <p>
 * Each client generates a random integer K in a limited range and creates keys in the form 'key-' + K + 'master',
 * 'key-' + K + '-1', 'key-' + K + '-2', ... Then client starts a pessimistic repeatable read transaction
 * and randomly chooses between read and write scenarios:
 * <ul>
 * <li>Reads value associated with the master key and child keys. Values must be equal.</li>
 * <li>Reads value associated with the master key, increments it by 1 and puts the value, then invokes increment
 * closure on child keys. No validation is performed.</li>
 * </ul>
 */
public class IgniteTransactionalWriteInvokeBenchmark extends IgniteFailoverAbstractBenchmark<String, Long> {
    /** */
    private static final Long INITIAL_VALUE = 1L;

    /** */
    public static final int TIMEOUT_SEC = 20 * 60;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        assert args.keysCount() > 0 : "Count of keys: " + args.keysCount();

        IgniteCountDownLatch latch = ignite().countDownLatch("DATA-POPULATED-LATCH-" + cacheName(), 1, true, true);

        if (cfg.memberId() == 0) {
            println(cfg, "Populating data for cache: " + cacheName());

            long start = System.nanoTime();

            try (IgniteDataStreamer<String, Long> dataLdr = ignite().dataStreamer(cacheName())) {
                for (int k = 0; k < args.range() && !Thread.currentThread().isInterrupted(); k++) {
                    dataLdr.addData("key-" + k + "-master", INITIAL_VALUE);

                    for (int i = 0; i < args.keysCount(); i++)
                        dataLdr.addData("key-" + k + "-" + i, INITIAL_VALUE);

                    if (k % 100000 == 0)
                        println(cfg, "Populated accounts: " + k);
                }
            }

            println(cfg, "Finished populating data in " + ((System.nanoTime() - start) / 1_000_000)
                + " ms. for cache: " + cacheName());

            latch.countDown();
        }
        else {
            println(cfg, "Waiting for populating data in cache by driver with id 0: " + cacheName());

            boolean success = latch.await(TIMEOUT_SEC, TimeUnit.SECONDS);

            if (!success)
                throw new IllegalStateException("Failed to wait that data populating finish.");

            println(cfg, "Finished waiting for populating data in cache by driver with id 0: " + cacheName());
        }
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        final int k = nextRandom(args.range());

        final String[] keys = new String[args.keysCount()];

        final String masterKey = "key-" + k + "-master";

        for (int i = 0; i < keys.length; i++)
            keys[i] = "key-" + k + "-" + i;

        final int scenario = nextRandom(2);

        final Set<String> badKeys = new LinkedHashSet<>();

        doInTransaction(ignite().transactions(), PESSIMISTIC, REPEATABLE_READ, new Callable<Void>() {
            @Override public Void call() throws Exception {
                final int timeout = args.cacheOperationTimeoutMillis();

                switch (scenario) {
                    case 0: // Read scenario.
                        Map<String, Long> map = new HashMap<>();

                        Long cacheVal = cache.getAsync(masterKey).get(timeout);

                        map.put(masterKey, cacheVal);

                        for (String key : keys) {
                            cacheVal = cache.getAsync(key).get(timeout);

                            map.put(key, cacheVal);
                        }

                        Set<Long> values = new HashSet<>(map.values());

                        if (values.size() != 1)
                            throw new IgniteConsistencyException("Found different values for keys [map=" + map + "]");

                        break;
                    case 1: // Invoke scenario.
                        Long val = cache.getAsync(masterKey).get(timeout);

                        if (val == null)
                            badKeys.add(masterKey);

                        cache.putAsync(masterKey, val == null ? -1 : val + 1).get(timeout);

                        for (String key : keys) {
                            Object o = cache.invokeAsync(key,
                                new IncrementWriteInvokeCacheEntryProcessor(), cacheName()).get(timeout);

                            if (o != null)
                                badKeys.add(key);
                        }

                        break;
                }

                return null;
            }
        });

        if (!F.isEmpty(badKeys))
            throw new IgniteConsistencyException("Found unexpected null-value(s) for the following " +
                "key(s) (look for debug information on server nodes): " + badKeys);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected String cacheName() {
        return "tx-write-invoke";
    }

    /**
     */
    private static class IncrementWriteInvokeCacheEntryProcessor implements CacheEntryProcessor<String, Long, Object> {
        /** */
        private static final long serialVersionUID = 0;

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<String, Long> entry,
            Object... arguments) throws EntryProcessorException {
            if (entry.getValue() == null) {
                String cacheName = (String)arguments[0];

                IgniteKernal kernal = (IgniteKernal)entry.unwrap(Ignite.class);

                Affinity<String> aff = kernal.affinity(cacheName);

                final int partIdx = aff.partition(entry.getKey());

                final Collection<ClusterNode> nodes = aff.mapKeyToPrimaryAndBackups(entry.getKey());

                List<GridDhtLocalPartition> locPartitions = kernal.cachex(cacheName).context().topology().
                    localPartitions();

                GridDhtLocalPartition part = null;

                for (GridDhtLocalPartition p : locPartitions) {
                    if (p.id() == partIdx) {
                        part = p;

                        break;
                    }
                }

                kernal.log().warning("Found unexpected null-value, debug info:"
                        + "\n    entry=" + entry
                        + "\n    key=" + entry.getKey()
                        + "\n    locNodeId=" + kernal.cluster().localNode().id()
                        + "\n    primaryAndBackupsNodes=" + nodes
                        + "\n    part=" + part
                        + "\n    partIdx=" + partIdx
                        + "\n    locParts=" + locPartitions
                        + "\n    allPartMap=" + kernal.cachex(cacheName).context().topology().partitionMap(true)
                );

                return new Object(); // non-null value.
            }

            entry.setValue(entry.getValue() + 1);

            return null;
        }
    }
}
