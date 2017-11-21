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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.logger.java.JavaLogger;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_STW_ALLOWED_DURATION_LIMIT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_STW_DETECTOR_SLEEP_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 *
 */
class LongJVMPauseDetector {
    /** Logger. */
    private static final IgniteLogger LOG = new JavaLogger();

    /** Worker. */
    private static final Thread WORKER;

    /** Long pause count. */
    private static volatile long longPausesCount;

    /** Long pause total duration. */
    private static volatile long longPausesTotalDuration;

    static {
        WORKER = new Thread() {
            private long millis = System.currentTimeMillis();

            @Override public void run() {
                while (true) {
                    try {
                        final int timeout = getInteger(IGNITE_STW_DETECTOR_SLEEP_TIMEOUT, 10);

                        Thread.sleep(timeout);

                        final long now = System.currentTimeMillis();

                        final long pause = now - timeout - millis;

                        millis = now;

                        if (pause >= getInteger(IGNITE_STW_ALLOWED_DURATION_LIMIT, 100)) {
                            LOG.warning("Possible too long JVM pause: " + pause + " milliseconds.");

                            longPausesCount++;

                            longPausesTotalDuration += pause;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        };

        WORKER.setDaemon(true);
        WORKER.start();

        LOG.info("LongJVMPauseDetector has been initialized.");
    }

    /**
     *
     */
    static long longPausesCount() {
        return longPausesCount;
    }

    /**
     *
     */
    static long longPausesTotalDuration() {
        return longPausesTotalDuration;
    }
}
