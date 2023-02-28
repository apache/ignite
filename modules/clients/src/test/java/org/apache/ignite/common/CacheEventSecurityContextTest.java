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

package org.apache.ignite.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.ClientTransactions;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientData;
import org.apache.ignite.internal.client.GridClientDataConfiguration;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.google.common.collect.ImmutableSet.of;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_LOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_UNLOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_ADD;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_APPEND;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_CAS;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET_ALL;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET_AND_PUT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET_AND_REMOVE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET_AND_REPLACE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PREPEND;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT_ALL;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT_IF_ABSENT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REMOVE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REMOVE_ALL;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REMOVE_VALUE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REPLACE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REPLACE_VALUE;

/** Tests that security information specified in cache events belongs to the operation initiator. */
@RunWith(Parameterized.class)
public class CacheEventSecurityContextTest extends AbstractEventSecurityContextTest {
    /** Counter of inserted cache keys. */
    private static final AtomicInteger KEY_COUNTER = new AtomicInteger();

    /** Name of the cache with {@link CacheAtomicityMode#ATOMIC} mode. */
    private static final String ATOMIC_CACHE = "atomic";
    
    /** Name of the cache with {@link CacheAtomicityMode#TRANSACTIONAL} mode. */
    private static final String TRANSACTIONAL_CACHE = "transactional";

    /** {@inheritDoc} */
    @Override protected int[] eventTypes() {
        return new int[] {
            EVT_CACHE_OBJECT_PUT,
            EVT_CACHE_OBJECT_READ,
            EVT_CACHE_OBJECT_REMOVED,
            EVT_CACHE_OBJECT_LOCKED,
            EVT_CACHE_OBJECT_UNLOCKED,
            EVT_CACHE_QUERY_EXECUTED,
            EVT_CACHE_QUERY_OBJECT_READ
        };
    }

    /** */
    @Parameterized.Parameters(name = "cacheAtomicity={0}, txConcurrency={1}, txIsolation={2}")
    public static Iterable<Object[]> data() {
        List<Object[]> res = new ArrayList<>();

        for (TransactionConcurrency txConcurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation txIsolation : TransactionIsolation.values())
                res.add(new Object[] {TRANSACTIONAL_CACHE, txIsolation, txConcurrency});
        }

        res.add(new Object[] {TRANSACTIONAL_CACHE, null, null});
        res.add(new Object[] {ATOMIC_CACHE, null, null});

        return res;
    }

    /** */
    @Parameterized.Parameter()
    public String cacheName;

    /** */
    @Parameterized.Parameter(1)
    public TransactionIsolation txIsolation;

    /** */
    @Parameterized.Parameter(2)
    public TransactionConcurrency txConcurrency;

    /** */
    private String operationInitiatorLogin;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration<>(ATOMIC_CACHE).setAtomicityMode(ATOMIC), 
                new CacheConfiguration<>(TRANSACTIONAL_CACHE).setAtomicityMode(TRANSACTIONAL));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridAllowAll("crd");
        startGridAllowAll("srv");
        startClientAllowAll("cli");

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        grid("crd").cache(cacheName).clear();
    }

    /** Tests cache event security context in case operation is initiated from the {@link IgniteClient}. */
    @Test
    public void testIgniteClient() throws Exception {
        operationInitiatorLogin = THIN_CLIENT_LOGIN;

        ClientConfiguration cfg = new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setUserName(operationInitiatorLogin)
            .setUserPassword("");

        try (IgniteClient cli = Ignition.startClient(cfg)) {
            ClientCache<Integer, String> cache = cli.cache(cacheName);

            checkEvents(cli, k -> cache.put(k, "val"), false, EVT_CACHE_OBJECT_PUT);
            checkEvents(cli, k -> cache.putAsync(k, "val").get(), false, EVT_CACHE_OBJECT_PUT);

            checkEvents(cli, k -> cache.putAll(singletonMap(k, "val")), false, EVT_CACHE_OBJECT_PUT);
            checkEvents(cli, k -> cache.putAllAsync(singletonMap(k, "val")).get(), false, EVT_CACHE_OBJECT_PUT);

            checkEvents(cli, cache::remove, true, EVT_CACHE_OBJECT_REMOVED);
            checkEvents(cli, k -> cache.removeAsync(k).get(), true, EVT_CACHE_OBJECT_REMOVED);

            checkEvents(cli, k -> cache.remove(k, "val"), true, EVT_CACHE_OBJECT_REMOVED);

            // TODO Add test case inside transaction after resolving IGNITE-14317.
            checkEvents(k -> cache.removeAsync(k, "val").get(), true, EVT_CACHE_OBJECT_REMOVED);

            checkEvents(cli, k -> cache.removeAll(of(k)), true, EVT_CACHE_OBJECT_REMOVED);
            checkEvents(cli, k -> cache.removeAllAsync(of(k)).get(), true, EVT_CACHE_OBJECT_REMOVED);

            checkEvents(cli, k -> cache.removeAll(), true, EVT_CACHE_OBJECT_REMOVED);
            checkEvents(cli, k -> cache.removeAllAsync().get(), true, EVT_CACHE_OBJECT_REMOVED);

            checkEvents(cli, k -> cache.putIfAbsent(k, "val"), false, EVT_CACHE_OBJECT_PUT);
            checkEvents(cli, k -> cache.putIfAbsentAsync(k, "val").get(), false, EVT_CACHE_OBJECT_PUT);

            checkEvents(cli, k -> cache.getAndPut(k, "val"), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);
            checkEvents(cli, k -> cache.getAndPutAsync(k, "val").get(), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);

            checkEvents(cli, cache::get, true, EVT_CACHE_OBJECT_READ);
            checkEvents(cli, k -> cache.getAsync(k).get(), true, EVT_CACHE_OBJECT_READ);

            checkEvents(cli, k -> cache.getAll(of(k)), true, EVT_CACHE_OBJECT_READ);
            checkEvents(cli, k -> cache.getAllAsync(of(k)).get(), true, EVT_CACHE_OBJECT_READ);

            checkEvents(cli, k -> cache.getAndReplace(k, "val"), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);
            checkEvents(cli, k -> cache.getAndReplaceAsync(k, "val").get(), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);

            checkEvents(cli, cache::getAndRemove, true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_REMOVED);
            checkEvents(cli, k -> cache.getAndRemoveAsync(k).get(), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_REMOVED);

            checkEvents(cli, k -> cache.replace(k, "new_val"), true, EVT_CACHE_OBJECT_PUT);
            checkEvents(cli, k -> cache.replaceAsync(k, "new_val").get(), true, EVT_CACHE_OBJECT_PUT);

            checkEvents(cli, k -> cache.replace(k, "val", "new_val"), true, EVT_CACHE_OBJECT_PUT);
            checkEvents(cli, k -> cache.replaceAsync(k, "val", "new_val").get(), true, EVT_CACHE_OBJECT_PUT);

            checkEvents(() -> cache.query(new ScanQuery<>()).getAll(), asList(EVT_CACHE_QUERY_EXECUTED, EVT_CACHE_QUERY_OBJECT_READ));
        }
    }

    /** Tests cache event security context in case operation is initiated from the {@link GridClient}. */
    @Test
    public void testGridClient() throws Exception {
        Assume.assumeTrue(txIsolation == null && txConcurrency == null);

        operationInitiatorLogin = GRID_CLIENT_LOGIN;

        GridClientConfiguration cfg = new GridClientConfiguration()
            .setServers(singletonList("127.0.0.1:11211"))
            .setDataConfigurations(singletonList(new GridClientDataConfiguration().setName(cacheName)))
            .setSecurityCredentialsProvider(new SecurityCredentialsBasicProvider(new SecurityCredentials(operationInitiatorLogin, "")));

        try (GridClient cli = GridClientFactory.start(cfg)) {
            GridClientData cache = cli.data(cacheName);

            checkEvents(k -> cache.put(k, "val"), false, EVT_CACHE_OBJECT_PUT);
            checkEvents(k -> cache.putAsync(k, "val").get(), false, EVT_CACHE_OBJECT_PUT);

            checkEvents(k -> cache.putAll(singletonMap(k, "val")), false, EVT_CACHE_OBJECT_PUT);
            checkEvents(k -> cache.putAllAsync(singletonMap(k, "val")).get(), false, EVT_CACHE_OBJECT_PUT);

            checkEvents(cache::remove, true, EVT_CACHE_OBJECT_REMOVED);
            checkEvents(k -> cache.removeAsync(k).get(), true, EVT_CACHE_OBJECT_REMOVED);

            checkEvents(k -> cache.removeAll(of(k)), true, EVT_CACHE_OBJECT_REMOVED);
            checkEvents(k -> cache.removeAllAsync(of(k)).get(), true, EVT_CACHE_OBJECT_REMOVED);

            checkEvents(cache::get, true, EVT_CACHE_OBJECT_READ);
            checkEvents(k -> cache.getAsync(k).get(), true, EVT_CACHE_OBJECT_READ);

            checkEvents(k -> cache.getAll(of(k)), true, EVT_CACHE_OBJECT_READ);
            checkEvents(k -> cache.getAllAsync(of(k)).get(), true, EVT_CACHE_OBJECT_READ);

            checkEvents(k -> cache.replace(k, "val"), true, EVT_CACHE_OBJECT_PUT);
            checkEvents(k -> cache.replaceAsync(k, "val").get(), true, EVT_CACHE_OBJECT_PUT);

            checkEvents(k -> cache.append(k, "val"), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);
            checkEvents(k -> cache.appendAsync(k, "val").get(), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);

            checkEvents(k -> cache.prepend(k, "val"), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);
            checkEvents(k -> cache.prependAsync(k, "val").get(), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);

            checkEvents(k -> cache.cas(k, "new_val", "val"), true, EVT_CACHE_OBJECT_PUT);
            checkEvents(k -> cache.casAsync(k, "new_val", "val").get(), true, EVT_CACHE_OBJECT_PUT);
        }
    }

    /** Tests cache event security context in case operation is initiated from the REST client. */
    @Test
    public void testRestClient() throws Exception {
        Assume.assumeTrue(txIsolation == null && txConcurrency == null);

        operationInitiatorLogin = REST_CLIENT_LOGIN;

        checkEvents(k -> sendRestRequest(CACHE_PUT, k, "val", null), false, EVT_CACHE_OBJECT_PUT);
        checkEvents(k -> sendRestRequest(CACHE_PUT_ALL, k, "val", null), false, EVT_CACHE_OBJECT_PUT);
        checkEvents(k -> sendRestRequest(CACHE_REMOVE, k, null, null), true, EVT_CACHE_OBJECT_REMOVED);
        checkEvents(k -> sendRestRequest(CACHE_REMOVE_ALL, k, null, null), true, EVT_CACHE_OBJECT_REMOVED);
        checkEvents(k -> sendRestRequest(CACHE_GET, k, null, null), true, EVT_CACHE_OBJECT_READ);
        checkEvents(k -> sendRestRequest(CACHE_GET_ALL, k, null, null), true, EVT_CACHE_OBJECT_READ);
        checkEvents(k -> sendRestRequest(CACHE_REPLACE, k, "val", null), true, EVT_CACHE_OBJECT_PUT);
        checkEvents(k -> sendRestRequest(CACHE_APPEND, k, "val", null), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);
        checkEvents(k -> sendRestRequest(CACHE_PREPEND, k, "val", null), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);
        checkEvents(k -> sendRestRequest(CACHE_GET_AND_PUT, k, "val", null), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);
        checkEvents(k -> sendRestRequest(CACHE_GET_AND_REPLACE, k, "val", null), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);
        checkEvents(k -> sendRestRequest(CACHE_PUT_IF_ABSENT, k, "val", null), false, EVT_CACHE_OBJECT_PUT);
        checkEvents(k -> sendRestRequest(CACHE_GET_AND_REMOVE, k, "val", null), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_REMOVED);
        checkEvents(k -> sendRestRequest(CACHE_ADD, k, "val", null), false, EVT_CACHE_OBJECT_PUT);
        checkEvents(k -> sendRestRequest(CACHE_REMOVE_VALUE, k, "val", null), true, EVT_CACHE_OBJECT_REMOVED);
        checkEvents(k -> sendRestRequest(CACHE_REPLACE_VALUE, k, "new_val", "val"), true, EVT_CACHE_OBJECT_PUT);
        checkEvents(k -> sendRestRequest(CACHE_CAS, k, "new_val", "val"), true, EVT_CACHE_OBJECT_PUT);
    }

    /** Tests cache event security context in case operation is initiated from the server node. */
    @Test
    public void testServerNode() throws Exception {
        testNode(false);
    }

    /** Tests cache event security context in case operation is initiated from the client node. */
    @Test
    public void testClientNode() throws Exception {
        testNode(true);
    }

    /** */
    private void testNode(boolean isClient) throws Exception {
        operationInitiatorLogin = isClient ? "cli" : "srv";

        IgniteEx ignite = grid(operationInitiatorLogin);

        IgniteCache<Integer, String> cache = ignite.cache(cacheName);

        checkEvents(ignite, k -> cache.put(k, "val"), false, EVT_CACHE_OBJECT_PUT);
        checkEvents(ignite, k -> cache.putAsync(k, "val").get(), false, EVT_CACHE_OBJECT_PUT);

        checkEvents(ignite, k -> cache.putAll(singletonMap(k, "val")), false, EVT_CACHE_OBJECT_PUT);
        checkEvents(ignite, k -> cache.putAllAsync(singletonMap(k, "val")).get(), false, EVT_CACHE_OBJECT_PUT);

        checkEvents(ignite, cache::remove, true, EVT_CACHE_OBJECT_REMOVED);
        checkEvents(ignite, k -> cache.removeAsync(k).get(), true, EVT_CACHE_OBJECT_REMOVED);

        checkEvents(ignite, k -> cache.remove(k, "val"), true, EVT_CACHE_OBJECT_REMOVED);

        // TODO Add test case inside transaction after resolving IGNITE-14317.
        checkEvents(k -> cache.removeAsync(k, "val").get(), true, EVT_CACHE_OBJECT_REMOVED);

        checkEvents(ignite, k -> cache.removeAll(of(k)), true, EVT_CACHE_OBJECT_REMOVED);
        checkEvents(ignite, k -> cache.removeAllAsync(of(k)).get(), true, EVT_CACHE_OBJECT_REMOVED);

        checkEvents(ignite, k -> cache.removeAll(), true, EVT_CACHE_OBJECT_REMOVED);
        checkEvents(ignite, k -> cache.removeAllAsync().get(), true, EVT_CACHE_OBJECT_REMOVED);

        checkEvents(ignite, k -> cache.putIfAbsent(k, "val"), false, EVT_CACHE_OBJECT_PUT);
        checkEvents(ignite, k -> cache.putIfAbsentAsync(k, "val").get(), false, EVT_CACHE_OBJECT_PUT);

        checkEvents(ignite, k -> cache.getAndPut(k, "val"), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);
        checkEvents(ignite, k -> cache.getAndPutAsync(k, "val").get(), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);

        checkEvents(ignite, cache::get, true, EVT_CACHE_OBJECT_READ);
        checkEvents(ignite, k -> cache.getAsync(k).get(), true, EVT_CACHE_OBJECT_READ);

        checkEvents(ignite, k -> cache.getAll(of(k)), true, EVT_CACHE_OBJECT_READ);
        checkEvents(ignite, k -> cache.getAllAsync(of(k)).get(), true, EVT_CACHE_OBJECT_READ);

        checkEvents(ignite, k -> cache.getAndReplace(k, "val"), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);
        checkEvents(ignite, k -> cache.getAndReplaceAsync(k, "val").get(), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);

        checkEvents(ignite, cache::getAndRemove, true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_REMOVED);
        checkEvents(ignite, k -> cache.getAndRemoveAsync(k).get(), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_REMOVED);

        checkEvents(ignite, k -> cache.replace(k, "new_val"), true, EVT_CACHE_OBJECT_PUT);
        checkEvents(ignite, k -> cache.replaceAsync(k, "new_val").get(), true, EVT_CACHE_OBJECT_PUT);

        checkEvents(ignite, k -> cache.replace(k, "val", "new_val"), true, EVT_CACHE_OBJECT_PUT);
        checkEvents(ignite, k -> cache.replaceAsync(k, "val", "new_val").get(), true, EVT_CACHE_OBJECT_PUT);

        if (TRANSACTIONAL_CACHE.equals(cacheName)) {
            checkEvents(k -> {
                Lock lock = cache.lock(k);

                lock.lock();
                lock.unlock();

            }, true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_LOCKED, EVT_CACHE_OBJECT_UNLOCKED);
        }

        checkEvents(() -> cache.query(new ScanQuery<>()).getAll(), asList(EVT_CACHE_QUERY_EXECUTED, EVT_CACHE_QUERY_OBJECT_READ));

        CacheEntryProcessor<Integer, String, Void> proc = new CacheEntryProcessor<Integer, String, Void>() {
            @Override public Void process(MutableEntry<Integer, String> entry, Object... arguments) throws EntryProcessorException {
                entry.setValue(entry.getValue() + "_new_val");

                return null;
            }
        };

        checkEvents(ignite, k -> cache.invoke(k, proc), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);
        checkEvents(ignite, k -> cache.invokeAsync(k, proc).get(), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);

        checkEvents(ignite, k -> cache.invokeAll(of(k), proc), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);
        checkEvents(ignite, k -> cache.invokeAllAsync(of(k), proc).get(), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);

        checkEvents(ignite, k -> cache.invokeAll(singletonMap(k, proc)), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);
        checkEvents(ignite, k -> cache.invokeAllAsync(singletonMap(k, proc)), true, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT);
    }

    /**
     * Executes test operation considering whether it should be executed inside transaction in case {@link IgniteClient}
     * is operation initiator and checks events.
     */
    private void checkEvents(IgniteClient cli, ConsumerX<Integer> c, boolean withInitVal, Integer... expEvtTypes) throws Exception {
        checkEvents(txIsolation != null || txConcurrency != null ? key -> {
            ClientTransactions txs = cli.transactions();

            try (ClientTransaction tx = txs.txStart(txConcurrency, txIsolation)) {
                c.accept(key);

                tx.commit();
            }
        } : c, withInitVal, expEvtTypes);
    }

    /**
     * Executes test operation considering whether it should be executed inside transaction in case {@link Ignite} node
     * is operation initiator and checks events.
     */
    private void checkEvents(IgniteEx ignite, ConsumerX<Integer> op, boolean withInitVal, Integer... expEvtTypes) throws Exception {
        checkEvents(txIsolation != null || txConcurrency != null ? key -> {
            IgniteTransactions txs = ignite.transactions();

            try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
                op.accept(key);

                tx.commit();
            }
        } : op, withInitVal, expEvtTypes);
    }

    /** Executes specified cache operation on each server node and checks events. */
    private void checkEvents(ConsumerX<Integer> op, boolean withInitVal, Integer... expEvtTypes) throws Exception {
        IgniteEx crd = grid("crd");

        Set<Integer> evts = Arrays.stream(expEvtTypes).collect(Collectors.toSet());

        // TODO Remove the following workaround after resolving IGNITE-13490.
        if (ATOMIC_CACHE.equals(cacheName) && evts.size() > 1)
            evts.remove(EVT_CACHE_OBJECT_READ);

        if (TRANSACTIONAL_CACHE.equals(cacheName) && !(evts.size() == 1 && evts.contains(EVT_CACHE_OBJECT_READ))) {
            evts.add(EVT_CACHE_OBJECT_LOCKED);
            evts.add(EVT_CACHE_OBJECT_UNLOCKED);
        }

        List<Integer> keys = testNodes().stream().map(node ->
            keyForNode(
                crd.affinity(cacheName),
                KEY_COUNTER,
                node)
        ).collect(Collectors.toList());

        if (withInitVal)
            keys.forEach(key -> crd.cache(cacheName).put(key, "val"));

        checkEvents(() -> keys.forEach(op), evts);
    }

    /**
     * Checks that execution of specified {@link Runnable} raises events with the security subject ID owned by
     * operation initiator.
     */
    private void checkEvents(RunnableX op, Collection<Integer> expEvtTypes) throws Exception {
        checkEvents(
            op,
            expEvtTypes,
            operationInitiatorLogin);
    }

    /** */
    private void sendRestRequest(
        GridRestCommand cmd,
        Integer key,
        String val1,
        String val2
    ) throws Exception {
        List<String> params = new ArrayList<>();

        params.add("cacheName=" + cacheName);
        params.add("keyType=" + Integer.class.getName());
        params.add("valueType=" + String.class.getName());
        params.add((cmd == CACHE_PUT_ALL || cmd == CACHE_REMOVE_ALL || cmd == CACHE_GET_ALL ? "k1=" : "key=") + key);

        if (val1 != null)
            params.add((cmd == CACHE_PUT_ALL ? "v1=" : "val=") + val1);

        if (val2 != null)
            params.add("val2=" + val2);

        sendRestRequest(cmd, params, operationInitiatorLogin);
    }

    /** */
    @FunctionalInterface
    private static interface ConsumerX<T> extends Consumer<T> {
        /** */
        public void acceptX(T t) throws Exception;

        /** {@inheritDoc} */
        @Override public default void accept(T t) {
            try {
                acceptX(t);
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }
    }
}
