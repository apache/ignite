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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.util.lang.IgniteClosure2X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.junit.Test;

/**
 *
 */
public class CacheMvccScanQueryWithConcurrentTransactionTest extends CacheMvccAbstractFeatureTest {
    /**
     * @throws Exception if failed.
     */
    @Test
    public void testScanQuery() throws Exception {
        doTestConsistency(clo);
    }

    /** Test closure. */
    private final IgniteClosure2X<CountDownLatch, CountDownLatch, List<Person>> clo =
        new IgniteClosure2X<CountDownLatch, CountDownLatch, List<Person>>() {
        @Override public List<Person> applyx(CountDownLatch startLatch, CountDownLatch endLatch2)
            throws IgniteCheckedException {
            IgniteBiPredicate<Integer, Person> f = new IgniteBiPredicate<Integer, Person>() {
                @Override public boolean apply(Integer k, Person v) {
                    return k % 2 == 0;
                }
            };

            try (QueryCursor<Cache.Entry<Integer, Person>> cur = cache().query(new ScanQuery<Integer, Person>()
                .setFilter(f))) {
                Iterator<Cache.Entry<Integer, Person>> it = cur.iterator();

                List<Cache.Entry<Integer, Person>> pres = new ArrayList<>();

                for (int i = 0; i < 50; i++)
                    pres.add(it.next());

                if (startLatch != null)
                    startLatch.countDown();

                while (it.hasNext())
                    pres.add(it.next());

                if (endLatch2 != null)
                    U.await(endLatch2);

                return entriesToPersons(pres);
            }
        }
    };
}
