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

package org.apache.ignite.internal.processors.service;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.jsr166.ConcurrentHashMap8;

/**
 * Dummy service.
 */
public class DummyService implements Service {
    /** */
    private static final long serialVersionUID = 0L;

    /** Inited counter per service. */
    public static final ConcurrentMap<String, AtomicInteger> started = new ConcurrentHashMap8<>();

    /** Started counter per service. */
    public static final ConcurrentMap<String, AtomicInteger> inited = new ConcurrentHashMap8<>();

    /** Latches per service. */
    private static final ConcurrentMap<String, CountDownLatch> exeLatches = new ConcurrentHashMap8<>();

    /** Cancel latches per service. */
    private static final ConcurrentMap<String, CountDownLatch> cancelLatches = new ConcurrentHashMap8<>();

    /** Cancelled flags per service. */
    private static final ConcurrentMap<String, AtomicInteger> cancelled = new ConcurrentHashMap8<>();

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