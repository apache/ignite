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

package org.apache.ignite.internal.util;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Striped spin busy lock. Aimed to provide efficient "read" lock semantics while still maintaining safety when
 * entering "busy" state.
 */
public class GridStripedSpinBusyLock {
    /** Writer mask. */
    private static int WRITER_MASK = 1 << 30;

    /** Default amount of stripes. */
    private static final int DFLT_STRIPE_CNT = Runtime.getRuntime().availableProcessors() * 4;

    /** Thread index. */
    private static ThreadLocal<Integer> THREAD_IDX = new ThreadLocal<Integer>() {
        @Override protected Integer initialValue() {
            return new Random().nextInt(Integer.MAX_VALUE);
        }
    };

    /** States; they are not subjects to false-sharing because actual values are located far from each other. */
    private final AtomicInteger[] states;

    /**
     * Default constructor.
     */
    public GridStripedSpinBusyLock() {
        this(DFLT_STRIPE_CNT);
    }

    /**
     * Constructor.
     *
     * @param stripeCnt Amount of stripes.
     */
    public GridStripedSpinBusyLock(int stripeCnt) {
        states = new AtomicInteger[stripeCnt];

        for (int i = 0; i < stripeCnt; i++)
            states[i] = new AtomicInteger();
    }

    /**
     * Enter busy state.
     *
     * @return {@code True} if entered busy state.
     */
    public boolean enterBusy() {
        int val = state().incrementAndGet();

        if ((val & WRITER_MASK) == WRITER_MASK) {
            leaveBusy();

            return false;
        }
        else
            return true;
    }

    /**
     * Leave busy state.
     */
    public void leaveBusy() {
        state().decrementAndGet();
    }

    /**
     * Block.
     */
    public void block() {
        // 1. CAS-loop to set a writer bit.
        for (AtomicInteger state : states) {
            while (true) {
                int oldVal = state.get();

                if (state.compareAndSet(oldVal, oldVal | WRITER_MASK))
                    break;
            }
        }

        // 2. Wait until all readers are out.
        boolean interrupt = false;

        for (AtomicInteger state : states) {
            while (state.get() != WRITER_MASK) {
                try {
                    Thread.sleep(10);
                }
                catch (InterruptedException ignored) {
                    interrupt = true;
                }
            }
        }

        if (interrupt)
            Thread.currentThread().interrupt();
    }

    /**
     * Gets state of thread's stripe.
     *
     * @return State.
     */
    private AtomicInteger state() {
        return states[THREAD_IDX.get() % states.length];
    }
}
