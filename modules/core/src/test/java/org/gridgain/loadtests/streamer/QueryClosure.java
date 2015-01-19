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

package org.gridgain.loadtests.streamer;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Closure for events generation.
 */
class QueryClosure implements IgniteInClosure<IgniteStreamer> {
    /** Sleep period (seconds). */
    private static final int SLEEP_PERIOD_SEC = 3;

    /** Random range. */
    private int rndRange = 100;

    /** Warmup time. */
    private long warmup = 60000;

    /** {@inheritDoc} */
    @Override public void apply(IgniteStreamer streamer) {
        X.println("Pefromrming warmup: " + warmup + "ms...");

        try {
            U.sleep(warmup);
        }
        catch (GridInterruptedException ignore) {
            return;
        }

        long initTime = System.currentTimeMillis();
        long initExecs = streamer.metrics().stageTotalExecutionCount();

        long prevExecs = initExecs;

        while (!Thread.interrupted()) {
            try {
                U.sleep(SLEEP_PERIOD_SEC * 1000);
            }
            catch (GridInterruptedException ignore) {
                return;
            }

            long curTime = System.currentTimeMillis();
            long curExecs = streamer.metrics().stageTotalExecutionCount();

            long deltaExecs = curExecs - prevExecs;
            long deltaThroughput = deltaExecs/SLEEP_PERIOD_SEC;

            long totalTimeSec = (curTime - initTime) / 1000;
            long totalExecs = curExecs - initExecs;
            long totalThroughput = totalExecs/totalTimeSec;

            X.println("Measurement: [throughput=" + deltaThroughput + " execs/sec, totalThroughput=" +
                totalThroughput + " execs/sec]");

            prevExecs = curExecs;
        }
    }

    /**
     * @return Random range.
     */
    public int getRandomRange() {
        return rndRange;
    }

    /**
     * @param rndRange Random range.
     */
    public void setRandomRange(int rndRange) {
        this.rndRange = rndRange;
    }

    /**
     * @return Warmup time (milliseconds)
     */
    public long getWarmup() {
        return warmup;
    }

    /**
     * @param warmup Warmup time (milliseconds)
     */
    public void setWarmup(long warmup) {
        this.warmup = warmup;
    }
}
