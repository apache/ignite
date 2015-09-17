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

package org.apache.ignite.loadtests.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Random;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientData;
import org.apache.ignite.internal.client.GridClientDataConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.GridClientPartitionAffinity;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridFileLock;
import org.apache.ignite.testframework.GridLoadTestUtils;

/**
 * Use {@code modules/core/src/test/config/benchmark/spring-cache-client-benchmark-*.xml}
 * configurations for servers.
 */
public class ClientCacheBenchmark {
    /** Number of keys, used in PUT/GET operations. */
    private static final int KEY_COUNT = 1000;

    /** Size of arrays used as stored values. */
    private static final int VALUE_LENGTH = 1024*4;

    /** Cached values for store. */
    private static final byte[][] values = new byte[KEY_COUNT][];

    /** Probability of put operation. */
    private static final double WRITE_PROB = 0.2;

    /** Random generator. */
    private final Random rnd = new Random();

    /** Number of submitting threads for current test. */
    private final int threadCnt;

    /** Number of operations per thread. */
    private final int iterationCnt;

    /** Test client. */
    private GridClient client;

    /** Resulting number of iterations per second. */
    private double itersPerSec;

    /**
     * @param threadCnt Number of submitting threads.
     * @param iterationCnt Number of operations per thread.
     */
    public ClientCacheBenchmark(int threadCnt, int iterationCnt) {
        this.threadCnt = threadCnt;
        this.iterationCnt = iterationCnt;

        initValues();
    }

    /**
     * Initiates the values cache.
     */
    private void initValues() {
        for (int i = 0; i < KEY_COUNT; i++) {
            values[i] = new byte[VALUE_LENGTH];

            rnd.nextBytes(values[i]);
        }
    }

    /**
     * Performs test,
     *
     * @param printResults Whether to print results.
     * @throws GridClientException If failed.
     */
    @SuppressWarnings("NullableProblems")
    public void run(boolean printResults) throws GridClientException {
        Collection<TestThread> workers = new ArrayList<>(threadCnt);

        client = GridClientFactory.start(configuration());

        long startTime = System.currentTimeMillis();

        for(int i = 0; i < threadCnt; i++) {
            TestThread th = new TestThread();
            workers.add(th);
            th.start();
        }

        U.joinThreads(workers, null);

        if (printResults)
            countAndPrintSummary(workers, startTime);

        GridClientFactory.stopAll();
    }

    /**
     * @return Resulting iterations per second.
     */
    public double getItersPerSec() {
        return itersPerSec;
    }

    /**
     * Counts and prints tests summary,
     *
     * @param workers Collection of test worker threads.
     * @param startTime Time when test eas started.
     */
    private void countAndPrintSummary(Collection<TestThread> workers, long startTime) {
        long total = 0;

        int thCnt = workers.size();

        for (TestThread t : workers) { total += t.iters; }

        double timeSpent = ((double)(System.currentTimeMillis() - startTime)) / 1000;

        itersPerSec = total/timeSpent;

        System.out.printf("%8s, %12.0f, %12.0f, %12s\n", thCnt, itersPerSec, total/timeSpent/thCnt, total);
    }

    /**
     * @return Test client configuration.
     */
    private GridClientConfiguration configuration() {
        GridClientConfiguration cfg = new GridClientConfiguration();

        cfg.setServers(Collections.singleton("localhost:11211"));

        GridClientDataConfiguration cacheCfg = new GridClientDataConfiguration();

        cacheCfg.setName("partitioned");

        cacheCfg.setAffinity(new GridClientPartitionAffinity());

        cfg.setDataConfigurations(Collections.singletonList(cacheCfg));

        return cfg;
    }

    /**
     * Test thread.
     */
    private class TestThread extends Thread {
        /* Thread private random generator. */
        private final Random rnd = new Random();

        /** Number of iterations to perform. */
        private long iters;

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                GridClientData data = client.data("partitioned");

                for (int i = 0; i < iterationCnt; i++)
                    performIteration(data);
            }
            catch (GridClientException e) {
                e.printStackTrace();
            }
        }

        /**
         * Performs test iteration.
         * @param data client data to operate on.
         * @throws GridClientException If failed.
         */
        private void performIteration(GridClientData data) throws GridClientException {
            if (rnd.nextDouble() <= WRITE_PROB)
                data.put(rnd.nextInt(KEY_COUNT), values[rnd.nextInt(KEY_COUNT)]);
            else
                data.get(rnd.nextInt(KEY_COUNT));

            iters++;
        }
    }

    /**
     * Runs benchmark.
     * @param args Command-line arguments.
     * @throws GridClientException If failed.
     */
    public static void main(String[] args) throws GridClientException, IgniteCheckedException {
        GridFileLock fileLock = GridLoadTestUtils.fileLock();

        fileLock.lock();

        try {
            System.out.printf("%8s, %12s, %12s, %12s\n", "Threads", "It./s.", "It./s.*th.", "Iters.");

            if (args.length == 0) {
                for (int i = 1; i <= 16; i *= 2) {
                    ClientCacheBenchmark benchmark = new ClientCacheBenchmark(i, 10000);

                    benchmark.run(false);

                    System.gc();
                }

                for (int i = 1; i <= 64; i *= 2) {
                    ClientCacheBenchmark benchmark = new ClientCacheBenchmark(i, 10000);

                    benchmark.run(true);

                    System.gc();
                }
            }
            else {
                int nThreads = Integer.parseInt(args[0]);
                String outputFileName = (args.length >= 2 ? args[1] : null);

                ClientCacheBenchmark benchmark = null;

                for (int i = 0; i < 2; i++) {
                    benchmark = new ClientCacheBenchmark(nThreads, 10000);

                    benchmark.run(true);
                }

                if (outputFileName != null) {
                    X.println("Writing test results to a file: " + outputFileName);

                    assert benchmark != null;

                    try {
                        GridLoadTestUtils.appendLineToFile(
                            outputFileName,
                            "%s,%d",
                            GridLoadTestUtils.DATE_TIME_FORMAT.format(new Date()),
                            Math.round(benchmark.getItersPerSec()));
                    }
                    catch (IOException e) {
                        X.error("Failed to output to a file", e);
                    }
                }
            }
        }
        finally {
            fileLock.close();
        }
    }
}