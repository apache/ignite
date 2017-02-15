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

package org.apache.ignite.internal.processors.database;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.util.concurrent.TimeUnit;

/**
 * TODO: fix javadoc warnings, code style.
 */
public abstract class IgniteDbMemoryLeakAbstractTest extends IgniteDbAbstractTest {
    // TODO: take duration from system property.
    /** Test duration in seconds*/
    protected abstract int duration();

    @Override
    protected long getTestTimeout() {
        return duration() * 1200;
    }

    /** */
    protected abstract void operation(IgniteEx ig);

    /** */
    public void testMemoryLeak() throws Exception {
        // TODO: take PageMemory max size is the same as we configured.

        final long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(duration());

        // TODO: use threads instead of compute or make sure there are enough threads in pool.
        int tasksCnt = Runtime.getRuntime().availableProcessors() * 4;

        IgniteCompute compute = grid(0).compute().withAsync();

        ComputeTaskFuture[] futs = new ComputeTaskFuture[tasksCnt];

        for (int i = 0; i < tasksCnt; i++) {
            compute.run(new IgniteRunnable() {
                @IgniteInstanceResource
                private Ignite ig;

                @Override
                public void run() {
                    int i = 0;
                    while (System.nanoTime() < end) {
                        operation((IgniteEx) ig);

                        if(i++ == 100) {
                            check((IgniteEx) ig);
                            i = 0;
                        }
                    }
                }
            });

            futs[i] = compute.future();
        }

        for (ComputeTaskFuture fut : futs)
            fut.get();
    }

    protected void check(IgniteEx ig) {}
}
