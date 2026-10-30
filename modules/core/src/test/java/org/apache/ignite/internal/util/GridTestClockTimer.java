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

package org.apache.ignite.internal.util;

import java.util.function.LongSupplier;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Clock timer for tests.
 */
public class GridTestClockTimer implements Runnable {
    /** Default time supplier. */
    public static final LongSupplier DFLT_TIME_SUPPLIER = System::currentTimeMillis;

    /** Current time supplier. */
    private static volatile LongSupplier timeSupplier = DFLT_TIME_SUPPLIER;

    /** Mutex to avoid races between time updates. */
    private static final Object mux = new Object();

    /**
     * Constructor.
     */
    public GridTestClockTimer() {
        synchronized (IgniteUtils.mux) {
            // Nodes left running by a test that does not extend GridAbstractTest may have already started
            // the internal clock timer. The test clock replaces it.
            Thread timer = GridTestUtils.getFieldValue(CommonUtils.class, "timer");

            if (timer != null) {
                GridTestUtils.setFieldValue(CommonUtils.class, "timer", null);

                timer.interrupt();

                try {
                    timer.join();
                }
                catch (InterruptedException ignored) {
                    // The internal clock timer is already interrupted and will stop shortly. Do not throw:
                    // this constructor is called from GridAbstractTest static initializer, and a failure there
                    // breaks all subsequent tests.
                    Thread.currentThread().interrupt();
                }
            }

            IgniteUtils.gridCnt++; // To prevent one more timer thread start from IgniteUtils.onGridStart.
        }
    }

    /**
     * Sets new time supplier.
     *
     * @param timeSupplier Time supplier.
     */
    public static void timeSupplier(LongSupplier timeSupplier) {
        GridTestClockTimer.timeSupplier = timeSupplier;

        update();
    }

    /**
     * Updates current time with value supplied by time supplier.
     */
    public static void update() {
        synchronized (mux) {
            IgniteUtils.curTimeMillis = timeSupplier.getAsLong();
        }
    }

    /** {@inheritDoc} */
    @Override public void run() {
        while (true) {
            update();

            try {
                Thread.sleep(10);
            }
            catch (InterruptedException ignored) {
                break;
            }
        }
    }
}
