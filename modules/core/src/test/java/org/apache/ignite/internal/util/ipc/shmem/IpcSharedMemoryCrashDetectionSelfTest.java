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

package org.apache.ignite.internal.util.ipc.shmem;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.collections.CollectionUtils;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.ipc.IpcEndpointFactory;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Test shared memory endpoints crash detection.
 */
public class IpcSharedMemoryCrashDetectionSelfTest extends GridCommonAbstractTest {
    /** Timeout in ms between read/write attempts in busy-wait loops. */
    public static final int RW_SLEEP_TIMEOUT = 50;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IpcSharedMemoryNativeLoader.load(log());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        // Start and stop server endpoint to let GC worker
        // make a run and cleanup resources.
        IpcSharedMemoryServerEndpoint srv = new IpcSharedMemoryServerEndpoint(U.defaultWorkDirectory());

        new IgniteTestResources().inject(srv);

        try {
            srv.start();
        }
        finally {
            srv.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientThrowsCorrectExceptionUponServerKilling() throws Exception {
        info("Shared memory IDs before starting server-client interactions: " +
            IpcSharedMemoryUtils.sharedMemoryIds());

        Collection<Integer> shmemIdsWithinInteractions = checkClientThrowsCorrectExceptionUponServerKilling();

        Collection<Integer> shmemIdsAfterInteractions = IpcSharedMemoryUtils.sharedMemoryIds();

        info("Shared memory IDs created within interaction: " + shmemIdsWithinInteractions);
        info("Shared memory IDs after server killing and client graceful termination: " + shmemIdsAfterInteractions);

        assertFalse("List of shared memory IDs after killing server endpoint should not include IDs created " +
            "within server-client interactions.",
            CollectionUtils.containsAny(shmemIdsAfterInteractions, shmemIdsWithinInteractions));
    }

    /**
     * Launches SharedMemoryTestServer and connects to it with client endpoint.
     * After couple of reads-writes kills the server and checks client throws correct exception.
     *
     * @return List of shared memory IDs created while client-server interactions.
     * @throws Exception In case of any exception happen.
     */
    @SuppressWarnings("BusyWait")
    private Collection<Integer> checkClientThrowsCorrectExceptionUponServerKilling() throws Exception {
        ProcessStartResult srvStartRes = startSharedMemoryTestServer();

        Collection<Integer> shmemIds = new ArrayList<>();
        IpcSharedMemoryClientEndpoint client = null;

        int interactionsCntBeforeSrvKilling = 5;
        int i = 1;

        try {
            // Run client endpoint.
            client = (IpcSharedMemoryClientEndpoint) IpcEndpointFactory.connectEndpoint(
                "shmem:" + IpcSharedMemoryServerEndpoint.DFLT_IPC_PORT, log);

            OutputStream os = client.outputStream();

            shmemIds.add(client.inSpace().sharedMemoryId());
            shmemIds.add(client.outSpace().sharedMemoryId());

            for (; i < interactionsCntBeforeSrvKilling * 2; i++) {
                info("Write: 123");

                os.write(123);

                Thread.sleep(RW_SLEEP_TIMEOUT);

                if (i == interactionsCntBeforeSrvKilling) {
                    info("Going to kill server.");

                    srvStartRes.proc().kill();

                    info("Write 512k array to hang write procedure.");

                    os.write(new byte[512 * 1024]);
                }
            }

            fail("Client should throw IOException upon server killing.");
        }
        catch (IOException e) {
            assertTrue(i >= interactionsCntBeforeSrvKilling);

            assertTrue(X.hasCause(e, IgniteCheckedException.class));
            assertTrue(X.cause(e, IgniteCheckedException.class).getMessage().contains(
                "Shared memory segment has been closed"));
        }
        finally {
            U.closeQuiet(client);
        }

        srvStartRes.isKilledLatch().await();

        return shmemIds;
    }

    /**
     * Starts {@code SharedMemoryTestServer}. The method waits while server being started.
     *
     * @return Start result of the {@code SharedMemoryTestServer}.
     * @throws Exception In case of any exception happen.
     */
    private ProcessStartResult startSharedMemoryTestServer() throws Exception {
        final CountDownLatch srvReady = new CountDownLatch(1);
        final CountDownLatch isKilledLatch = new CountDownLatch(1);

        GridJavaProcess proc = GridJavaProcess.exec(
            SharedMemoryTestServer.class, null,
            log,
            new CI1<String>() {
                @Override public void apply(String str) {
                    info("Server process prints: " + str);

                    if (str.contains("IPC shared memory server endpoint started"))
                        srvReady.countDown();
                }
            },
            new CA() {
                @Override public void apply() {
                    info("Server is killed");

                    isKilledLatch.countDown();
                }
            },
            null,
            System.getProperty("surefire.test.class.path")
        );

        srvReady.await();

        ProcessStartResult res = new ProcessStartResult();

        res.proc(proc);
        res.isKilledLatch(isKilledLatch);

        return res;
    }

    /**
     * Internal utility class to store results of running client/server in separate process.
     */
    private static class ProcessStartResult {
        /** Java process within which some class has been run. */
        private GridJavaProcess proc;

        /** Count down latch to signal when process termination will be detected. */
        private CountDownLatch killedLatch;

        /** Count down latch to signal when process is readiness (in terms of business logic) will be detected. */
        private CountDownLatch readyLatch;

        /** Shared memory IDs string read from system.input. */
        private Collection<Integer> shmemIds;

        /**
         * @return Java process within which some class has been run.
         */
        GridJavaProcess proc() {
            return proc;
        }

        /**
         * Sets Java process within which some class has been run.
         *
         * @param proc Java process.
         */
        void proc(GridJavaProcess proc) {
            this.proc = proc;
        }

        /**
         * @return Latch to signal when process termination will be detected.
         */
        CountDownLatch isKilledLatch() {
            return killedLatch;
        }

        /**
         * Sets CountDownLatch to signal when process termination will be detected.
         *
         * @param killedLatch CountDownLatch
         */
        void isKilledLatch(CountDownLatch killedLatch) {
            this.killedLatch = killedLatch;
        }

        /**
         * @return Latch to signal when process is readiness (in terms of business logic) will be detected.
         */
        CountDownLatch isReadyLatch() {
            return readyLatch;
        }

        /**
         * Sets CountDownLatch to signal when process readiness (in terms of business logic) will be detected.
         *
         * @param readyLatch CountDownLatch
         */
        void isReadyLatch(CountDownLatch readyLatch) {
            this.readyLatch = readyLatch;
        }

        /**
         * @return Shared memory IDs string read from system.input. Nullable.
         */
        @Nullable Collection<Integer> shmemIds() {
            return shmemIds;
        }

        /**
         * Sets Shared memory IDs string read from system.input.
         *
         * @param shmemIds Shared memory IDs string.
         */
        public void shmemIds(String shmemIds) {
            this.shmemIds = (shmemIds == null) ? null :
                F.transform(shmemIds.split(","), new C1<String, Integer>() {
                    @Override public Integer apply(String s) {
                        return Long.valueOf(s).intValue();
                    }
                });
        }
    }
}
