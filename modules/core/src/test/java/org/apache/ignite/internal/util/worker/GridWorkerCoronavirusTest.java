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

package org.apache.ignite.internal.util.worker;

import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThread;
import org.junit.Test;

/**
 * Test to check worker coronavirus infection and death.
 */
public class GridWorkerCoronavirusTest extends GridCommonAbstractTest {
    /** Test logger. */
    private final ListeningTestLogger log = new ListeningTestLogger(false, GridAbstractTest.log);

    /** */
    @Test
    public void testDeathFromCoronavirus() throws Exception {
        LogListener lsnr = LogListener.matches(s -> s.contains("Sorry, I was died of COVID-19, bye!"))
                .atLeast(1).build();

        log.registerListener(lsnr);

        GridWorker worker = new GridWorker(null, "Infected worker", log) {
            @Override
            protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
                log.info("I'm alive and do work");
            }
        };
        worker.coronavirusInfectionProbability = 1.0;
        worker.coronavirusDeathProbability = 1.0;

        IgniteThread thread = new IgniteThread(worker);
        thread.start();
        thread.interrupt();
        thread.join();

        assertTrue(lsnr.check());
    }
}
