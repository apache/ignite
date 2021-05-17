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

package org.apache.ignite.internal.processors.security.events;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDataConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.RestQueryRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_CREATED;
import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_DESTROYED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_LOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_UNLOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;

/**
 * Tests that an event's local listener and an event's remote filter get correct subjectId when cache's operations are
 * performed.
 */
@RunWith(Parameterized.class)
public class CacheEventsTest extends AbstractCacheEventsTest {
    /** Cache atomicity mode. */
    @Parameterized.Parameter()
    public CacheAtomicityMode atomicMode;

    /** Expected login. */
    @Parameterized.Parameter(1)
    public String expLogin;

    /** Event type. */
    @Parameterized.Parameter(2)
    public int evtType;

    /** Test operation. */
    @Parameterized.Parameter(3)
    public TestOperation op;

    /** Parameters. */
    @Parameterized.Parameters(name = "atomicMode={0}, expLogin={1}, evtType={2}, op={3}")
    public static Iterable<Object[]> data() {
        List<Object[]> lst = Arrays.asList(
            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT},
            new Object[] {"thin", EVT_CACHE_OBJECT_PUT, TestOperation.PUT},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.PUT},
            new Object[] {"grid", EVT_CACHE_OBJECT_PUT, TestOperation.PUT},

            new Object[] {CLNT, EVT_CACHE_ENTRY_CREATED, TestOperation.PUT},
            new Object[] {SRV, EVT_CACHE_ENTRY_CREATED, TestOperation.PUT},
            new Object[] {"thin", EVT_CACHE_ENTRY_CREATED, TestOperation.PUT},
            new Object[] {"rest", EVT_CACHE_ENTRY_CREATED, TestOperation.PUT},
            new Object[] {"grid", EVT_CACHE_ENTRY_CREATED, TestOperation.PUT},

            new Object[] {CLNT, EVT_CACHE_ENTRY_DESTROYED, TestOperation.PUT},
            new Object[] {SRV, EVT_CACHE_ENTRY_DESTROYED, TestOperation.PUT},
            new Object[] {"thin", EVT_CACHE_ENTRY_DESTROYED, TestOperation.PUT},
            new Object[] {"rest", EVT_CACHE_ENTRY_DESTROYED, TestOperation.PUT},
            new Object[] {"grid", EVT_CACHE_ENTRY_DESTROYED, TestOperation.PUT},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ASYNC},
            new Object[] {"grid", EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL},
            new Object[] {"thin", EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL},
            new Object[] {"grid", EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL_ASYNC},
            new Object[] {"grid", EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT},
            new Object[] {"thin", EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT_ASYNC},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET},
            new Object[] {"thin", EVT_CACHE_OBJECT_READ, TestOperation.GET},
            new Object[] {"rest", EVT_CACHE_OBJECT_READ, TestOperation.GET},
            new Object[] {"grid", EVT_CACHE_OBJECT_READ, TestOperation.GET},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ASYNC},
            new Object[] {"grid", EVT_CACHE_OBJECT_READ, TestOperation.GET_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ENTRY},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ENTRY},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ENTRY_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ENTRY_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ENTRIES},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ENTRIES},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ENTRIES_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ENTRIES_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL},
            new Object[] {"thin", EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL},
            new Object[] {"rest", EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL},
            new Object[] {"grid", EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_ASYNC},
            new Object[] {"grid", EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_OUT_TX},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_OUT_TX},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_OUT_TX_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_OUT_TX_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT},
            new Object[] {"thin", EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT},
            new Object[] {"rest", EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT},
            new Object[] {"thin", EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_IF_ABSENT},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_IF_ABSENT},
            new Object[] {"rest", EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_IF_ABSENT},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_IF_ABSENT_PUT_CASE},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_IF_ABSENT_PUT_CASE},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_IF_ABSENT_PUT_CASE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_IF_ABSENT_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_IF_ABSENT_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_IF_ABSENT_PUT_CASE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_IF_ABSENT_PUT_CASE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REMOVE},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REMOVE},
            new Object[] {"thin", EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REMOVE},
            new Object[] {"rest", EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REMOVE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.GET_AND_REMOVE},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.GET_AND_REMOVE},
            new Object[] {"thin", EVT_CACHE_OBJECT_REMOVED, TestOperation.GET_AND_REMOVE},
            new Object[] {"rest", EVT_CACHE_OBJECT_REMOVED, TestOperation.GET_AND_REMOVE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REMOVE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REMOVE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.GET_AND_REMOVE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.GET_AND_REMOVE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE},
            new Object[] {"thin", EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE},
            new Object[] {"rest", EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE},
            new Object[] {"grid", EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ASYNC},
            new Object[] {"grid", EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_VAL},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_VAL},
            new Object[] {"thin", EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_VAL},
            new Object[] {"rest", EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_VAL},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_VAL_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_VAL_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL},
            new Object[] {"thin", EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL},
            new Object[] {"rest", EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL},
            new Object[] {"grid", EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL_ASYNC},
            new Object[] {"grid", EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE},
            new Object[] {"thin", EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE},
            new Object[] {"grid", EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_ASYNC},
            new Object[] {"grid", EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL},
            new Object[] {"thin", EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL},
            new Object[] {"grid", EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL_ASYNC},
            new Object[] {"grid", EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REPLACE},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REPLACE},
            new Object[] {"thin", EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REPLACE},
            new Object[] {"rest", EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REPLACE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REPLACE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REPLACE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_REPLACE},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_REPLACE},
            new Object[] {"thin", EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_REPLACE},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_REPLACE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_REPLACE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_REPLACE_ASYNC},

            new Object[] {"grid", EVT_CACHE_OBJECT_PUT, TestOperation.APPEND},
            new Object[] {"grid", EVT_CACHE_OBJECT_PUT, TestOperation.APPEND_ASYNC},

            new Object[] {"grid", EVT_CACHE_OBJECT_PUT, TestOperation.PREPEND},
            new Object[] {"grid", EVT_CACHE_OBJECT_PUT, TestOperation.PREPEND_ASYNC},

            new Object[] {SRV, EVT_CACHE_OBJECT_LOCKED, TestOperation.LOCK},
            new Object[] {CLNT, EVT_CACHE_OBJECT_LOCKED, TestOperation.LOCK},
            new Object[] {SRV, EVT_CACHE_OBJECT_UNLOCKED, TestOperation.LOCK},
            new Object[] {CLNT, EVT_CACHE_OBJECT_UNLOCKED, TestOperation.LOCK},

            new Object[] {SRV, EVT_CACHE_OBJECT_LOCKED, TestOperation.LOCK_ALL},
            new Object[] {CLNT, EVT_CACHE_OBJECT_LOCKED, TestOperation.LOCK_ALL},
            new Object[] {SRV, EVT_CACHE_OBJECT_UNLOCKED, TestOperation.LOCK_ALL},
            new Object[] {CLNT, EVT_CACHE_OBJECT_UNLOCKED, TestOperation.LOCK_ALL},

            new Object[] {CLNT, EVT_CACHE_QUERY_EXECUTED, TestOperation.SCAN_QUERY},
            new Object[] {SRV, EVT_CACHE_QUERY_EXECUTED, TestOperation.SCAN_QUERY},
            new Object[] {"thin", EVT_CACHE_QUERY_EXECUTED, TestOperation.SCAN_QUERY},
            new Object[] {"rest", EVT_CACHE_QUERY_EXECUTED, TestOperation.SCAN_QUERY},

            new Object[] {CLNT, EVT_CACHE_QUERY_OBJECT_READ, TestOperation.SCAN_QUERY},
            new Object[] {SRV, EVT_CACHE_QUERY_OBJECT_READ, TestOperation.SCAN_QUERY},
            new Object[] {"thin", EVT_CACHE_QUERY_OBJECT_READ, TestOperation.SCAN_QUERY},
            new Object[] {"rest", EVT_CACHE_QUERY_OBJECT_READ, TestOperation.SCAN_QUERY}
        );

        List<Object[]> res = new ArrayList<>();

        for (Object[] arr : lst) {
            res.add(new Object[] {CacheAtomicityMode.TRANSACTIONAL, arr[0], arr[1], arr[2]});

            int evt = (int)arr[1];

            TestOperation op = (TestOperation)arr[2];
            // todo IGNITE-13490 Cache's operation getAndXXX doesn't trigger a CacheEvent with type EVT_CACHE_OBJECT_READ.
            // This condition should be removed when the issue will be done.
            if (evt == EVT_CACHE_OBJECT_READ && op.name().contains("GET_AND_"))
                continue;

            if (evt == EVT_CACHE_OBJECT_LOCKED || evt == EVT_CACHE_OBJECT_UNLOCKED)
                continue;

            res.add(new Object[] {CacheAtomicityMode.ATOMIC, arr[0], arr[1], arr[2]});
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode cacheAtomicityMode() {
        return atomicMode;
    }

    /** {@inheritDoc} */
    @Override protected String expectedLogin() {
        return expLogin;
    }

    /** {@inheritDoc} */
    @Override protected int eventType() {
        return evtType;
    }

    /** {@inheritDoc} */
    @Override protected TestOperation testOperation() {
        return op;
    }

    /**
     * @return Operation to test.
     */
    @Override protected GridTestUtils.ConsumerX<String> operation() {
        if ("rest".equals(expLogin)) {
            if (op == TestOperation.SCAN_QUERY) {
                RestQueryRequest req = new RestQueryRequest();

                req.credentials(new SecurityCredentials("rest", ""));
                req.command(op.restCmd);
                req.queryType(RestQueryRequest.QueryType.SCAN);
                req.pageSize(256);

                return n -> {
                    req.cacheName(n);

                    restProtocolHandler(grid(LISTENER_NODE)).handle(req);
                };
            }
            else {
                final GridRestCacheRequest req = new GridRestCacheRequest();

                req.credentials(new SecurityCredentials("rest", ""));
                req.key(KEY);
                req.command(op.restCmd);

                if (op == TestOperation.REPLACE_VAL) {
                    req.value(VAL);
                    req.value2(INIT_VAL);
                }
                else if (op == TestOperation.REMOVE_VAL)
                    req.value(INIT_VAL);
                else {
                    req.value(VAL);
                    req.values(F.asMap(KEY, VAL));
                }

                return n -> {
                    req.cacheName(n);

                    restProtocolHandler(grid(LISTENER_NODE)).handle(req);
                };
            }
        }
        else if ("thin".equals(expLogin)) {
            return n -> {
                try (IgniteClient clnt = startClient()) {
                    op.clntCacheOp.accept(clnt.cache(n));
                }
            };
        }
        else if ("grid".equals(expLogin)) {
            return n -> {
                try (GridClient clnt = startGridClient(n)) {
                    op.gridClntDataOp.accept(clnt.data(n));
                }
            };
        }
        else
            return n -> op.ignCacheOp.accept(grid(expLogin).cache(n));
    }

    /** */
    private IgniteClient startClient() {
        return Ignition.startClient(
            new ClientConfiguration().setAddresses(Config.SERVER)
                .setUserName(expLogin)
                .setUserPassword("")
        );
    }

    /** */
    private GridClient startGridClient(String cacheName) throws GridClientException {
        GridClientConfiguration cfg = new GridClientConfiguration()
            .setServers(singletonList("127.0.0.1:11211"))
            .setSecurityCredentialsProvider(new SecurityCredentialsBasicProvider(new SecurityCredentials("grid", "")))
            .setBalancer(nodes ->
                nodes.stream().findFirst().orElseThrow(NoSuchElementException::new));

        GridClientDataConfiguration ccfg = new GridClientDataConfiguration();

        ccfg.setName(cacheName);

        cfg.setDataConfigurations(singleton(ccfg));

        return GridClientFactory.start(cfg);
    }
}
