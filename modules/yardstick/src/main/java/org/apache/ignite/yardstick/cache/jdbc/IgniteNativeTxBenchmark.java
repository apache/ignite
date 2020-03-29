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

package org.apache.ignite.yardstick.cache.jdbc;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.cache.model.Accounts;
import org.apache.ignite.yardstick.cache.model.Branches;
import org.apache.ignite.yardstick.cache.model.History;
import org.apache.ignite.yardstick.cache.model.Tellers;
import org.yardstickframework.BenchmarkConfiguration;

/** JDBC benchmark that performs raw SQL insert */
public class IgniteNativeTxBenchmark extends IgniteAbstractBenchmark {
    /** Default number of rows in Accounts table. */
    private long accRows;

    /** Default number of rows in Tellers table. */
    private long tellRows;

    /** Default number of rows in Branches table. */
    private long branchRows;

    /** Cache for Accounts table. */
    private IgniteCache<Long, Accounts> accounts;

    /** Cache for Tellers table. */
    private IgniteCache<Long, Tellers> tellers;

    /** Cache for Branches table. */
    private IgniteCache<Long, Branches> branches;

    /** Cache for History table. */
    private IgniteCache<Long, History> hist;

    /** Id for History table */
    private AtomicLong cnt;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        cnt = new AtomicLong();

        accRows = 1000L * args.scaleFactor();
        tellRows = 10L * args.scaleFactor();
        branchRows = 5L * args.scaleFactor();

        accounts = ignite().cache("Accounts");
        tellers = ignite().cache("Tellers");
        branches = ignite().cache("Branches");
        hist = ignite().cache("History");

        clearCaches();

        fillTables();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SimplifiableIfStatement")
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        long aid = ThreadLocalRandom.current().nextLong(accRows);
        long bid = ThreadLocalRandom.current().nextLong(branchRows);
        long tid = ThreadLocalRandom.current().nextLong(tellRows);

        final long delta = ThreadLocalRandom.current().nextLong(1000);

        IgniteTransactions transactions = ignite().transactions();

        try (Transaction tx = transactions.txStart(args.txConcurrency(), args.txIsolation())) {
            accounts.invoke(aid, new EntryProcessor<Long, Accounts, Object>() {
                @Override public Long process(MutableEntry<Long, Accounts> entry, Object... objects)
                    throws EntryProcessorException {
                    long newVal = entry.getValue().getVal() + delta;

                    entry.setValue(entry.getValue().setVal(newVal));

                    return newVal;
                }
            });

            tellers.invoke(tid, new EntryProcessor<Long, Tellers, Object>() {
                @Override public Long process(MutableEntry<Long, Tellers> entry, Object... objects)
                    throws EntryProcessorException {
                    long newVal = entry.getValue().getVal() + delta;

                    entry.setValue(entry.getValue().setVal(newVal));

                    return null;
                }
            });

            branches.invoke(bid, new EntryProcessor<Long, Branches, Object>() {
                @Override public Long process(MutableEntry<Long, Branches> entry,
                    Object... objects) throws EntryProcessorException {
                    long newVal = entry.getValue().getVal() + delta;

                    entry.setValue(entry.getValue().setVal(newVal));

                    return null;
                }
            });

            hist.put(cnt.getAndIncrement(), new History(tid, bid, aid, delta));

            tx.commit();
        }

        return true;
    }

    /**
     * Fill tables using native Ignite API.
     */
    private void fillTables() throws Exception {
        startPreloadLogging(args.preloadLogsInterval());

        try (IgniteDataStreamer<Long, Accounts> dataLdr = ignite().dataStreamer(accounts.getName())) {
            for (long i = 0; i < accRows; i++)
                dataLdr.addData(i, new Accounts(nextRandom(args.range())));
        }

        try (IgniteDataStreamer<Long, Branches> dataLdr = ignite().dataStreamer(branches.getName())) {
            for (long i = 0; i < branchRows; i++)
                dataLdr.addData(i, new Branches(nextRandom(args.range())));
        }

        try (IgniteDataStreamer<Long, Tellers> dataLdr = ignite().dataStreamer(tellers.getName())) {
            for (long i = 0; i < tellRows; i++)
                dataLdr.addData(i, new Tellers(nextRandom(args.range())));
        }

        stopPreloadLogging();
    }

    /**
     * Clear caches.
     */
    private void clearCaches() {
        ignite().cache("Accounts").clear();
        ignite().cache("Tellers").clear();
        ignite().cache("Branches").clear();
        ignite().cache("History").clear();
    }
}
