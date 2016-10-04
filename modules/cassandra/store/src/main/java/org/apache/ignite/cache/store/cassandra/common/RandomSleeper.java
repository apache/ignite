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

package org.apache.ignite.cache.store.cassandra.common;

import java.util.Random;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;

/**
 * Provides sleep method with randomly selected sleep time from specified range and
 * incrementally shifts sleep time range for each next sleep attempt
 *
 */
public class RandomSleeper {
    /** */
    private int min;

    /** */
    private int max;

    /** */
    private int incr;

    /** */
    private IgniteLogger log;

    /** */
    private Random random = new Random(System.currentTimeMillis());

    /** */
    private int summary;

    /**
     * Creates sleeper instance.
     *
     * @param min minimum sleep time (in milliseconds)
     * @param max maximum sleep time (in milliseconds)
     * @param incr time range shift increment (in milliseconds)
     */
    public RandomSleeper(int min, int max, int incr, IgniteLogger log) {
        if (min <= 0)
            throw new IllegalArgumentException("Incorrect min time specified: " + min);

        if (max <= min)
            throw new IllegalArgumentException("Incorrect max time specified: " + max);

        if (incr < 10)
            throw new IllegalArgumentException("Incorrect increment specified: " + incr);

        this.min = min;
        this.max = max;
        this.incr = incr;
        this.log = log;
    }

    /**
     * Sleeps
     */
    public void sleep() {
        try {
            int timeout = random.nextInt(max - min + 1) + min;

            if (log != null)
                log.info("Sleeping for " + timeout + "ms");

            Thread.sleep(timeout);

            summary += timeout;

            if (log != null)
                log.info("Sleep completed");
        }
        catch (InterruptedException e) {
            throw new IgniteException("Random sleep interrupted", e);
        }

        min += incr;
        max += incr;
    }

    /**
     * Returns summary sleep time.
     *
     * @return Summary sleep time in milliseconds.
     */
    public int getSleepSummary() {
        return summary;
    }
}
