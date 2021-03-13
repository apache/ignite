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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_CREATED;
import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_DESTROYED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;
import static org.apache.ignite.internal.processors.security.events.AbstractCacheEventsTest.TestOperation.GET;

/**
 * Security subject id of transaction cache events have to refer to subject that initiates cache operation.
 */
@RunWith(Parameterized.class)
public class CacheEventsInsideTransactionTest extends AbstractCacheEventsTest {
    /** Expected login. */
    @Parameterized.Parameter()
    public String expLogin;

    /** Event type. */
    @Parameterized.Parameter(1)
    public int evtType;

    /** Test operation. */
    @Parameterized.Parameter(2)
    public TestOperation op;

    /** Transaction concurrency. */
    @Parameterized.Parameter(3)
    public TransactionConcurrency txCncr;

    /** Transaction isolation. */
    @Parameterized.Parameter(4)
    public TransactionIsolation txIsl;

    /** Parameters. */
    @Parameterized.Parameters(name = "expLogin={0}, evtType={1}, op={2}, txCncr={3}, txIsl={4}")
    public static Iterable<Object[]> data() {
        List<Object[]> lst = Arrays.asList(
            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT},

            new Object[] {CLNT, EVT_CACHE_ENTRY_CREATED, TestOperation.PUT},
            new Object[] {SRV, EVT_CACHE_ENTRY_CREATED, TestOperation.PUT},

            new Object[] {CLNT, EVT_CACHE_ENTRY_DESTROYED, TestOperation.PUT},
            new Object[] {SRV, EVT_CACHE_ENTRY_DESTROYED, TestOperation.PUT},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, GET},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, GET},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ASYNC},

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

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_OUT_TX},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_OUT_TX},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_OUT_TX_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_OUT_TX_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_IF_ABSENT},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_IF_ABSENT},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_IF_ABSENT_PUT_CASE},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_IF_ABSENT_PUT_CASE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_IF_ABSENT_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_IF_ABSENT_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_IF_ABSENT_PUT_CASE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_IF_ABSENT_PUT_CASE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REMOVE},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REMOVE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.GET_AND_REMOVE},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.GET_AND_REMOVE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REMOVE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REMOVE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.GET_AND_REMOVE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.GET_AND_REMOVE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_VAL},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_VAL},

            //todo не работает для оптимистик транзакций, нужно сделать репродюсер и создать задачу с ошибкой.
            //https://github.com/apache/ignite/pull/8841 репродюссер
            //new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, CacheEventsTest.TestOperation.REMOVE_VAL_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_VAL_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REPLACE},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REPLACE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REPLACE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REPLACE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_REPLACE},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_REPLACE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_REPLACE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_REPLACE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_QUERY_EXECUTED, TestOperation.SCAN_QUERY},
            new Object[] {SRV, EVT_CACHE_QUERY_EXECUTED, TestOperation.SCAN_QUERY},

            new Object[] {CLNT, EVT_CACHE_QUERY_OBJECT_READ, TestOperation.SCAN_QUERY},
            new Object[] {SRV, EVT_CACHE_QUERY_OBJECT_READ, TestOperation.SCAN_QUERY},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.INVOKE},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.INVOKE},
            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.INVOKE},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.INVOKE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.INVOKE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.INVOKE_ASYNC},
            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.INVOKE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.INVOKE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.INVOKE_ALL_SET},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.INVOKE_ALL_SET},
            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.INVOKE_ALL_SET},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.INVOKE_ALL_SET},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.INVOKE_ALL_SET_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.INVOKE_ALL_SET_ASYNC},
            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.INVOKE_ALL_SET_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.INVOKE_ALL_SET_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.INVOKE_ALL_MAP},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.INVOKE_ALL_MAP},
            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.INVOKE_ALL_MAP},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.INVOKE_ALL_MAP},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.INVOKE_ALL_MAP_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.INVOKE_ALL_MAP_ASYNC},
            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.INVOKE_ALL_MAP_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.INVOKE_ALL_MAP_ASYNC}
        );

        List<Object[]> res = new ArrayList<>();

        for (Object[] arr : lst) {
            for (TransactionConcurrency txc : TransactionConcurrency.values()) {
                for (TransactionIsolation txi : TransactionIsolation.values())
                    res.add(new Object[] {arr[0], arr[1], arr[2], txc, txi});
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode cacheAtomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
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

    /** {@inheritDoc} */
    @Override protected GridTestUtils.ConsumerX<String> operation() {
        return n -> {
            Ignite ignite = grid(expectedLogin());

            IgniteCache<String, String> cache = ignite.cache(n);

            IgniteTransactions transactions = ignite.transactions();

            try (Transaction tx = transactions.txStart(txCncr, txIsl)) {
                testOperation().ignCacheOp.accept(cache);

                tx.commit();
            }
        };
    }
}
