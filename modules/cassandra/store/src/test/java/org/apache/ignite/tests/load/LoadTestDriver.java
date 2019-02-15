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

package org.apache.ignite.tests.load;

import java.lang.reflect.Constructor;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.cassandra.common.SystemHelper;
import org.apache.ignite.tests.utils.TestsHelper;
import org.apache.log4j.Logger;

/**
 * Basic load test driver to be inherited by specific implementation for particular use-case.
 */
public abstract class LoadTestDriver {
    /** Number of attempts to setup load test */
    private static final int NUMBER_OF_SETUP_ATTEMPTS = 10;

    /** Timeout between load test setup attempts */
    private static final int SETUP_ATTEMPT_TIMEOUT = 1000;

    /** */
    public void runTest(String testName, Class<? extends Worker> clazz, String logName) {
        logger().info("Running " + testName + " test");

        Object cfg = null;

        int attempt;

        logger().info("Setting up load tests driver");

        for (attempt = 0; attempt < NUMBER_OF_SETUP_ATTEMPTS; attempt++) {
            try {
                cfg = setup(logName);
                break;
            }
            catch (Throwable e) {
                logger().error((attempt + 1) + " attempt to setup load test '" + testName + "' failed", e);
            }

            if (attempt + 1 != NUMBER_OF_SETUP_ATTEMPTS) {
                logger().info("Sleeping for " + SETUP_ATTEMPT_TIMEOUT + " seconds before trying next attempt " +
                        "to setup '" + testName + "' load test");

                try {
                    Thread.sleep(SETUP_ATTEMPT_TIMEOUT);
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }
            }
        }

        if (cfg == null && attempt == NUMBER_OF_SETUP_ATTEMPTS) {
            throw new RuntimeException("All " + NUMBER_OF_SETUP_ATTEMPTS + " attempts to setup load test '" +
                    testName+ "' have failed");
        }

        // calculates host unique prefix based on its subnet IP address
        long hostUniquePrefix = getHostUniquePrefix();

        logger().info("Load tests driver setup successfully completed");

        try {

            List<Worker> workers = new LinkedList<>();
            long startPosition = 0;

            logger().info("Starting workers");

            for (int i = 0; i < TestsHelper.getLoadTestsThreadsCount(); i++) {
                Worker worker = createWorker(clazz, cfg,
                    hostUniquePrefix + startPosition,
                    hostUniquePrefix + startPosition + 100000000);
                workers.add(worker);
                worker.setName(testName + "-worker-" + i);
                worker.start();
                startPosition += 100000001;
            }

            logger().info("Workers started");
            logger().info("Waiting for workers to complete");

            List<String> failedWorkers = new LinkedList<>();

            for (Worker worker : workers) {
                boolean failed = false;

                try {
                    worker.join();
                }
                catch (Throwable e) {
                    logger().error("Worker " + worker.getName() + " waiting interrupted", e);
                    failed = true;
                }

                if (failed || worker.isFailed()) {
                    failedWorkers.add(worker.getName());
                    logger().info("Worker " + worker.getName() + " execution failed");
                }
                else
                    logger().info("Worker " + worker.getName() + " successfully completed");
            }

            printTestResultsHeader(testName, failedWorkers);
            printTestResultsStatistics(testName, workers);
        }
        finally {
            tearDown(cfg);
        }
    }

    /** */
    protected abstract Logger logger();

    /** */
    protected abstract Object setup(String logName);

    /** */
    protected void tearDown(Object obj) {
    }

    /** */
    @SuppressWarnings("unchecked")
    private Worker createWorker(Class clazz, Object cfg, long startPosition, long endPosition) {
        try {
            Class cfgCls = cfg instanceof Ignite ? Ignite.class : CacheStore.class;

            Constructor ctor = clazz.getConstructor(cfgCls, long.class, long.class);

            return (Worker)ctor.newInstance(cfg, startPosition, endPosition);
        }
        catch (Throwable e) {
            logger().error("Failed to instantiate worker of class '" + clazz.getName() + "'", e);
            throw new RuntimeException("Failed to instantiate worker of class '" + clazz.getName() + "'", e);
        }
    }

    /** */
    private void printTestResultsHeader(String testName, List<String> failedWorkers) {
        if (failedWorkers.isEmpty()) {
            logger().info(testName + " test execution successfully completed.");
            return;
        }

        if (failedWorkers.size() == TestsHelper.getLoadTestsThreadsCount()) {
            logger().error(testName + " test execution totally failed.");
            return;
        }

        String strFailedWorkers = "";

        for (String workerName : failedWorkers) {
            if (!strFailedWorkers.isEmpty())
                strFailedWorkers += ", ";

            strFailedWorkers += workerName;
        }

        logger().warn(testName + " test execution completed, but " + failedWorkers.size() + " of " +
            TestsHelper.getLoadTestsThreadsCount() + " workers failed. Failed workers: " + strFailedWorkers);
    }

    /** */
    @SuppressWarnings("StringBufferReplaceableByString")
    private void printTestResultsStatistics(String testName, List<Worker> workers) {
        long cnt = 0;
        long errCnt = 0;
        long speed = 0;

        for (Worker worker : workers) {
            cnt += worker.getMsgProcessed();
            errCnt += worker.getErrorsCount();
            speed += worker.getSpeed();
        }

        float errPercent = errCnt == 0 ?
            0 :
            cnt + errCnt ==  0 ? 0 : (float)(errCnt * 100 ) / (float)(cnt + errCnt);

        StringBuilder builder = new StringBuilder();
        builder.append(SystemHelper.LINE_SEPARATOR);
        builder.append("-------------------------------------------------");
        builder.append(SystemHelper.LINE_SEPARATOR);
        builder.append(testName).append(" test statistics").append(SystemHelper.LINE_SEPARATOR);
        builder.append(testName).append(" messages: ").append(cnt).append(SystemHelper.LINE_SEPARATOR);
        builder.append(testName).append(" errors: ").append(errCnt).append(", ").
                append(String.format("%.2f", errPercent).replace(",", ".")).
                append("%").append(SystemHelper.LINE_SEPARATOR);
        builder.append(testName).append(" speed: ").append(speed).append(" msg/sec").append(SystemHelper.LINE_SEPARATOR);
        builder.append("-------------------------------------------------");

        logger().info(builder.toString());
    }

    /** */
    private long getHostUniquePrefix() {
        String[] parts = SystemHelper.HOST_IP.split("\\.");

        if (parts[2].equals("0"))
            parts[2] = "777";

        if (parts[3].equals("0"))
            parts[3] = "777";

        long part3 = Long.parseLong(parts[2]);
        long part4 = Long.parseLong(parts[3]);

        if (part3 < 10)
            part3 *= 100;
        else if (part4 < 100)
            part3 *= 10;

        if (part4 < 10)
            part4 *= 100;
        else if (part4 < 100)
            part4 *= 10;

        return (part4 * 100000000000000L) + (part3 * 100000000000L) + Thread.currentThread().getId();
    }
}
