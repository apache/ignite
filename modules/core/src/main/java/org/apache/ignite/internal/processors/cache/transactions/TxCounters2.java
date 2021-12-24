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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Values which should be tracked during transaction execution and applied on commit.
 */
public class TxCounters2 {
    /** */
    public static final int INITIAL_SIZE = 1;

    /** */
    private final int[][] caches = new int[INITIAL_SIZE][3];

    /** */
    private final AtomicLong[] cntrs = new AtomicLong[INITIAL_SIZE];

    /** */
    private int cnt = -1;

    /** */
    private int atomicCntr = -1;

    /** */
    public TxCounters2() {
    }

    /**
     * @param cache Cache id.
     * @param part Partition number.
     */
    public void incrementUpdateCounter(int cache, int part) {
        int idx = 0;

        while (idx < cnt && idx < caches.length && (caches[idx][0] != cache || caches[idx][1] != part))
            idx++;

        if (caches[idx][0] != cache || caches[idx][1] != part) {
            caches[idx][0] = cache;
            caches[idx][1] = part;
            caches[idx][2] = ++atomicCntr;

            cntrs[caches[idx][2]] = new AtomicLong();

            cnt++;
        }

        cntrs[caches[idx][2]].incrementAndGet();
    }
}
