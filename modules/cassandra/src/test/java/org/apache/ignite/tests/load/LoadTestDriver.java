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

package org.apache.ignite.tests.load;

import java.lang.reflect.Constructor;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.cassandra.utils.common.SystemHelper;
import org.apache.ignite.tests.utils.TestsHelper;
import org.apache.log4j.Logger;

/**
 * Basic load test driver to be inherited by specific implementation for particular use-case
 */
public abstract class LoadTestDriver {
    public void runTest(String testName, Class<? extends Worker> clazz, String loggerName) {
        logger().info("Running " + testName + " test");

        Object config = setup(loggerName);

        try {

            List<Worker> workers = new LinkedList<>();
            int startPosition = 0;

            logger().info("Starting workers");

            for (int i = 0; i < TestsHelper.getLoadTestsThreadsCount(); i++) {
                Worker worker = createWorker(clazz, config, startPosition, startPosition + 10000000);
                workers.add(worker);
                worker.setName(testName + "-worker-" + i);
                worker.start();
                startPosition += 10000001;
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

                if (failed || worker.isFailed())
                    failedWorkers.add(worker.getName());
            }

            printTestResultsHeader(testName, failedWorkers);
            printTestResultsStatistics(testName, workers);
        }
        finally {
            tearDown(config);
        }
    }

    protected abstract Logger logger();

    protected abstract Object setup(String loggerName);

    protected void tearDown(Object obj) {
    }

    @SuppressWarnings("unchecked")
    private Worker createWorker(Class clazz, Object config, int startPosition, int endPosition) {
        try {
            Class configClass = config instanceof Ignite ? Ignite.class : CacheStore.class;
            Constructor constr = clazz.getConstructor(configClass, int.class, int.class);
            return (Worker)constr.newInstance(config, startPosition, endPosition);
        }
        catch (Throwable e) {
            logger().error("Failed to instantiate worker of class '" + clazz.getName() + "'", e);
            throw new RuntimeException("Failed to instantiate worker of class '" + clazz.getName() + "'", e);
        }
    }

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

    @SuppressWarnings("StringBufferReplaceableByString")
    private void printTestResultsStatistics(String testName, List<Worker> workers) {
        int count = 0;
        int speed = 0;

        for (Worker worker : workers) {
            count += worker.getMsgCountTotal();
            speed += worker.getSpeed();
        }

        StringBuilder builder = new StringBuilder();
        builder.append(SystemHelper.LINE_SEPARATOR);
        builder.append("-------------------------------------------------");
        builder.append(SystemHelper.LINE_SEPARATOR);
        builder.append(testName).append(" test statistics").append(SystemHelper.LINE_SEPARATOR);
        builder.append("Messages: ").append(count).append(SystemHelper.LINE_SEPARATOR);
        builder.append("Speed: ").append(speed).append(" msg/sec").append(SystemHelper.LINE_SEPARATOR);
        builder.append("-------------------------------------------------");

        logger().info(builder.toString());
    }
}
