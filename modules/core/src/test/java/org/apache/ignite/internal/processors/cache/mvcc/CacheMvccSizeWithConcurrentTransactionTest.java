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

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.IgniteClosure2X;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class CacheMvccSizeWithConcurrentTransactionTest extends CacheMvccAbstractFeatureTest {
    /**
     * @throws Exception if failed.
     */
    public void testSize() throws Exception {
        doTestConsistency(clo);
    }

    /** Test closure. */
    private final IgniteClosure2X<CountDownLatch, CountDownLatch, Integer> clo =
        new IgniteClosure2X<CountDownLatch, CountDownLatch, Integer>() {
        @Override public Integer applyx(CountDownLatch startLatch, CountDownLatch endLatch2)
            throws IgniteCheckedException {
            if (startLatch != null)
                startLatch.countDown();

            int res = cache().size();

            if (endLatch2 != null)
                U.await(endLatch2);

            return res;
        }
    };
}
