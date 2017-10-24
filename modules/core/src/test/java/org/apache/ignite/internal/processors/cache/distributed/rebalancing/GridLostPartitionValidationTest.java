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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_VALIDATOR;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Tests that cache operations validated.
 */
public class GridLostPartitionValidationTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "test";

    /** Tx cache name. */
    private static final String TX_CACHE_NAME = "tx_test";

    /** Backups. */
    private int backups;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setBackups(backups);

        CacheConfiguration ccfg2 = new CacheConfiguration(TX_CACHE_NAME);

        ccfg2.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg2.setBackups(backups);
        ccfg2.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfg, ccfg2);

        cfg.setIncludeEventTypes(EVT_CACHE_REBALANCE_PART_DATA_LOST);

        Map<IgnitePredicate<? extends Event>, int[]> listeners = new HashMap<>();

        listeners.put(new Listener(), new int[]{EVT_CACHE_REBALANCE_PART_DATA_LOST});

        cfg.setLocalEventListeners(listeners);

        cfg.setClientMode(gridName.contains("client"));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.clearProperty(IGNITE_CACHE_VALIDATOR);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_CACHE_VALIDATOR, CacheValidator.class.getName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartDataLostEvent1Backup() throws Exception {
        backups = 1;

        checkValidator();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    private void checkValidator() throws Exception {
        List<Ignite> srvrs = new ArrayList<>();

        srvrs.add(startGrid("server-0"));

        final Ignite client = startGrid("client");

        srvrs.add(startGrid("server-1"));
        srvrs.add(startGrid("server-2"));

        awaitPartitionMapExchange();

        final IgniteCache<Object, Object> cache = client.cache(CACHE_NAME);
        final IgniteCache<Object, Object> txCache = client.cache(TX_CACHE_NAME);

        for (int i = 0; i < 10_000; i++) {
            cache.put(i, i);
            txCache.put(i, i);
        }

        // Stop node with 0 partition.
        Set<ClusterNode> nodes = new HashSet<>(client.affinity(CACHE_NAME).mapPartitionToPrimaryAndBackups(0));

        List<String> stopped = stopAffinityNodes(srvrs, nodes);

        awaitPartitionMapExchange();

        for (Iterator<Ignite> iter = srvrs.iterator(); iter.hasNext(); ) {
            Ignite srvr = iter.next();

            if (stopped.contains(srvr.name()))
                iter.remove();
        }

        Ignite srvr = F.first(srvrs);

        checkThrows(client);
        checkThrows(srvr);
    }

    /**
     * @param ignite Client.
     */
    @SuppressWarnings("ThrowableNotThrown")
    private void checkThrows(final Ignite ignite) {
        final IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);
        final IgniteCache<Object, Object> txCache = ignite.cache(TX_CACHE_NAME);

        boolean client = ignite.cluster().localNode().isClient();

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                cache.query(new ScanQuery<>()).getAll();

                return null;
            }
        });

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                cache.get(0);

                return null;
            }
        });

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                cache.put(0, 0);

                return null;
            }
        });

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                cache.remove(0);

                return null;
            }
        });

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                cache.invoke(0, new CacheEntryProcessor<Object, Object, Object>() {
                    @Override public Object process(MutableEntry<Object, Object> entry,
                        Object... arguments) throws EntryProcessorException {

                        return null;
                    }
                });

                return null;
            }
        });

        if (!client) {
            assertThrows(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    cache.localPeek(0);

                    return null;
                }
            });
        }

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Lock lock = cache.lock(0);

                lock.lock();

                return null;
            }
        });

        if (!client) {
            assertThrows(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    for (Cache.Entry<Object, Object> entry : cache.localEntries())
                        System.out.println(entry);

                    return null;
                }
            });
        }

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                txCache.query(new ScanQuery<>()).getAll();

                return null;
            }
        });

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                txCache.get(0);

                return null;
            }
        });

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                txCache.put(0, 0);

                return null;
            }
        });

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                txCache.remove(0);

                return null;
            }
        });

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                txCache.invoke(0, new CacheEntryProcessor<Object, Object, Object>() {
                    @Override public Object process(MutableEntry<Object, Object> entry,
                        Object... arguments) throws EntryProcessorException {
                        return null;
                    }
                });

                return null;
            }
        });

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
                    txCache.get(0);

                    tx.commit();
                }

                return null;
            }
        });

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
                    txCache.put(0, 0);

                    tx.commit();
                }

                return null;
            }
        });

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
                    txCache.remove(0);

                    tx.commit();
                }

                return null;
            }
        });

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
                    txCache.invoke(0, new CacheEntryProcessor<Object, Object, Object>() {
                        @Override public Object process(MutableEntry<Object, Object> entry,
                            Object... arguments) throws EntryProcessorException {
                            return null;
                        }
                    });

                    tx.commit();
                }

                return null;
            }
        });

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                    txCache.get(0);

                    tx.commit();
                }

                return null;
            }
        });

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                    txCache.put(0, 0);

                    tx.commit();
                }

                return null;
            }
        });

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                    txCache.remove(0);

                    tx.commit();
                }

                return null;
            }
        });

        assertThrows(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                    txCache.invoke(0, new CacheEntryProcessor<Object, Object, Object>() {
                        @Override public Object process(MutableEntry<Object, Object> entry,
                            Object... arguments) throws EntryProcessorException {

                            return null;
                        }
                    });

                    tx.commit();
                }

                return null;
            }
        });

        if (!client) {
            assertThrows(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    txCache.localPeek(0);

                    return null;
                }
            });
        }

        if (!client) {
            assertThrows(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    for (Cache.Entry<Object, Object> entry : txCache.localEntries())
                        System.out.println(entry);

                    return null;
                }
            });
        }
    }

    /**
     * @param c Closure.
     */
    private void assertThrows(Callable<?> c) {
        try {
            c.call();

            assert false : "Exception was not thrown";
        }
        catch (CacheException | IgniteException e) {
            log.info("Caught expected exception: " + e.getClass());
        }
        catch (Exception e) {
            e.printStackTrace();

            assert false : "Wrong exception was thrown: " + e.getClass();
        }
    }

    /**
     * @param srvrs Servers.
     * @param nodes Nodes.
     */
    @NotNull private List<String> stopAffinityNodes(List<Ignite> srvrs, Set<ClusterNode> nodes) throws IgniteCheckedException {
        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        final List<String> stopped = new ArrayList<>();

        for (final Ignite srv : srvrs) {
            final ClusterNode node = srv.cluster().localNode();

            if (nodes.contains(node)) {
                IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        srv.close();

                        System.out.println(">> Stopped " + srv.name() + " " + node.id());

                        stopped.add(srv.name());

                        return null;
                    }
                });

                futs.add(fut);
            }
        }

        for (IgniteInternalFuture<?> fut : futs)
            fut.get();

        return stopped;
    }

    /**
     *
     */
    private static class Listener implements IgnitePredicate<CacheRebalancingEvent> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Ignite. */
        @SuppressWarnings("unused")
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public boolean apply(CacheRebalancingEvent evt) {
            ignite.cluster().nodeLocalMap().putIfAbsent(evt.cacheName(), false);

            return true;
        }
    }

    /**
     *
     */
    public static class CacheValidator implements IgniteClosure<String, Throwable> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Logger. */
        @SuppressWarnings("unused")
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Throwable apply(String cacheName) {
            log.info(">>> Validator");

            Object val = ignite.cluster().nodeLocalMap().get(cacheName);

            return Boolean.FALSE.equals(val) ? new IllegalStateException("Illegal cache state "
                + Thread.currentThread().getName()) : null;
        }
    }
}
