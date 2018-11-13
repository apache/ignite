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

package org.apache.ignite.yardstick.jdbc.mvcc;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.query.IgniteSQLException;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Benchmark app that eliminates update contention in mvcc mode.
 * Designed to be ran in many threads on many hosts.
 */
public class MvccUpdateContentionBenchmark extends AbstractDistributedMvccBenchmark {
    /** Expected expception message in mvcc on mode on update fail. */
    private static final String MVCC_EXC_MSG = "Cannot serialize transaction due to write conflict";

    /** Expected exception message in mvcc off mode on update fail. */
    private static final String NO_MVCC_EXC_MSG_PREFIX =
        "Failed to UPDATE some keys because they had been modified concurrently";

    /** Counter of failed updates. */
    private final AtomicLong failsCnt = new AtomicLong();

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long start = rnd.nextLong(args.mvccContentionRange() - (args.sqlRange() - 1)) + 1;

        long end = start + (args.sqlRange() - 1);

        try {
            execute(new SqlFieldsQuery(UPDATE_QRY).setArgs(start, end));
        }
        catch (IgniteSQLException exc) {
            if ((args.atomicMode() == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT && !exc.getMessage().startsWith(MVCC_EXC_MSG)) ||
                (args.atomicMode() != CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT && !exc.getMessage().startsWith(NO_MVCC_EXC_MSG_PREFIX)))
                throw new RuntimeException("Exception with unexpected message is thrown.", exc);

            failsCnt.incrementAndGet();
        }
        catch (Exception e) {
            throw new RuntimeException("Could not perform update.", e);
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        try {
            super.tearDown();
        }
        finally {
            println("Update contention count : " + failsCnt.get());
        }
    }
}
