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

package org.apache.ignite.internal.processors.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

/**
 * Dummy service.
 */
public class DummyService implements Service {
    /** */
    private static final long serialVersionUID = 0L;

    /** Inited counter per service. */
    public static final ConcurrentMap<String, AtomicInteger> started = new ConcurrentHashMap<>();

    /** Started counter per service. */
    public static final ConcurrentMap<String, AtomicInteger> inited = new ConcurrentHashMap<>();

    /** Latches per service. */
    private static final ConcurrentMap<String, CountDownLatch> exeLatches = new ConcurrentHashMap<>();

    /** Cancel latches per service. */
    private static final ConcurrentMap<String, CountDownLatch> cancelLatches = new ConcurrentHashMap<>();

    /** Cancelled flags per service. */
    private static final ConcurrentMap<String, AtomicInteger> cancelled = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public void cancel(ServiceContext ctx) {
        AtomicInteger cntr = cancelled.get(ctx.name());

        if (cntr == null) {
            AtomicInteger old = cancelled.putIfAbsent(ctx.name(), cntr = new AtomicInteger());

            if (old != null)
                cntr = old;
        }

        cntr.incrementAndGet();

        CountDownLatch latch = cancelLatches.get(ctx.name());

        if (latch != null)
            latch.countDown();

        System.out.println("Cancelling service: " + ctx.name());
    }

    /** {@inheritDoc} */
    @Override public void init(ServiceContext ctx) throws Exception {
        AtomicInteger cntr = inited.get(ctx.name());

        if (cntr == null) {
            AtomicInteger old = inited.putIfAbsent(ctx.name(), cntr = new AtomicInteger());

            if (old != null)
                cntr = old;
        }

        cntr.incrementAndGet();

        System.out.println("Initializing service: " + ctx.name());
    }

    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) {
        AtomicInteger cntr = started.get(ctx.name());

        if (cntr == null) {
            AtomicInteger old = started.putIfAbsent(ctx.name(), cntr = new AtomicInteger());

            if (old != null)
                cntr = old;
        }

        cntr.incrementAndGet();

        System.out.println("Executing service: " + ctx.name());

        CountDownLatch latch = exeLatches.get(ctx.name());

        if (latch != null)
            latch.countDown();
    }

    /**
     * @param name Service name.
     * @return Cancelled flag.
     */
    public static int cancelled(String name) {
        AtomicInteger cntr = cancelled.get(name);

        return cntr == null ? 0 : cntr.get();
    }

    /**
     * @param name Service name.
     * @return Started counter.
     */
    public static int started(String name) {
        AtomicInteger cntr = started.get(name);

        return cntr == null ? 0 : cntr.get();
    }

    /**
     * Resets dummy service to initial state.
     */
    public static void reset() {
        System.out.println("Resetting dummy service.");

        started.clear();
        exeLatches.clear();
        cancelled.clear();
    }

    /**
     * @param name Service name.
     * @param latch Count down latch.
     */
    public static void exeLatch(String name, CountDownLatch latch) {
        exeLatches.put(name, latch);
    }

    /**
     * @param name Service name.
     * @param latch Cancel latch.
     */
    public static void cancelLatch(String name, CountDownLatch latch) {
        cancelLatches.put(name, latch);
    }
}