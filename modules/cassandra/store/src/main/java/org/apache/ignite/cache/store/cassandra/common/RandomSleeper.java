/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
