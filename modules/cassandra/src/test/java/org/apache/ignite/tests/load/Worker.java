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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.cassandra.utils.common.SystemHelper;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.tests.utils.TestsHelper;
import org.apache.log4j.Logger;

/**
 * Worker thread abstraction to be inherited by specific load test implementation
 */
public abstract class Worker extends Thread {
    private static final SimpleDateFormat TIME_FORMATTER = new SimpleDateFormat("hh:mm:ss");

    private long testStartTime;

    boolean warmup = TestsHelper.getLoadTestsWarmupPeriod() != 0;

    private volatile long warmupStartTime = 0;
    private volatile long warmupFinishTime = 0;

    private volatile long startTime = 0;
    private volatile long finishTime = 0;

    private volatile int warmupMsgProcessed = 0;
    private volatile int warmupSleepCount = 0;

    private volatile int msgProcessed = 0;
    private volatile int sleepCount = 0;

    private Throwable executionError;

    private long statReportedTime;

    private CacheStore cacheStore;
    private Ignite ignite;
    private IgniteCache igniteCache;

    private Logger logger;
    private int startPosition;
    private int endPosition;

    public Worker(CacheStore cacheStore, int startPosition, int endPosition) {
        this.cacheStore = cacheStore;
        this.logger = Logger.getLogger(loggerName());
        this.startPosition = startPosition;
        this.endPosition = endPosition;
    }

    public Worker(Ignite ignite, int startPosition, int endPosition) {
        this.ignite = ignite;
        this.logger = Logger.getLogger(loggerName());
        this.startPosition = startPosition;
        this.endPosition = endPosition;
    }

    @SuppressWarnings("unchecked")
    @Override public void run() {
        try {
            if (cacheStore != null)
                execute();
            else {
                igniteCache = ignite.getOrCreateCache(new CacheConfiguration(TestsHelper.getLoadTestsCacheName()));
                execute();
            }
        }
        catch (Throwable e) {
            executionError = e;
            throw new RuntimeException("Test execution abnormally terminated", e);
        }
    }

    public boolean isFailed() {
        return executionError != null;
    }

    public int getSpeed() {
        if (msgProcessed == 0)
            return 0;

        long finish = finishTime != 0 ? finishTime : System.currentTimeMillis();
        long duration = (finish - startTime - sleepCount * TestsHelper.getLoadTestsRequestsLatency()) / 1000;

        return duration == 0 ? msgProcessed : msgProcessed / (int)duration;
    }

    public int getMsgCountTotal() {
        return warmupMsgProcessed + msgProcessed;
    }

    protected abstract String loggerName();

    protected abstract boolean batchMode();

    protected void process(CacheStore cacheStore, CacheEntryImpl entry) {
        throw new UnsupportedOperationException("Single message processing is not supported");
    }

    protected void process(IgniteCache cache, Object key, Object value) {
        throw new UnsupportedOperationException("Single message processing is not supported");
    }

    protected void process(CacheStore cacheStore, Collection<CacheEntryImpl> entries) {
        throw new UnsupportedOperationException("Batch processing is not supported");
    }

    protected void process(IgniteCache cache, Map map) {
        throw new UnsupportedOperationException("Batch processing is not supported");
    }

    @SuppressWarnings("unchecked")
    private void execute() throws InterruptedException {
        testStartTime = System.currentTimeMillis();

        logger.info("Test execution started");

        if (warmup)
            logger.info("Warm up period started");

        warmupStartTime = warmup ? testStartTime : 0;
        startTime = !warmup ? testStartTime : 0;

        statReportedTime = testStartTime;

        int counter = startPosition;
        Object key = TestsHelper.generateLoadTestsKey(counter);
        Object val = TestsHelper.generateLoadTestsValue(counter);
        List<CacheEntryImpl> batchList = new ArrayList<>(TestsHelper.getBulkOperationSize());
        Map batchMap = new HashMap(TestsHelper.getBulkOperationSize());

        try {
            while (true) {
                if (System.currentTimeMillis() - testStartTime > TestsHelper.getLoadTestsExecutionTime()) {
                    break;
                }

                if (warmup && System.currentTimeMillis() - testStartTime > TestsHelper.getLoadTestsWarmupPeriod()) {
                    warmupFinishTime = System.currentTimeMillis();
                    startTime = warmupFinishTime;
                    warmup = false;
                    logger.info("Warm up period completed");
                }

                if (!batchMode()) {
                    if (cacheStore != null)
                        doWork(new CacheEntryImpl(key, val));
                    else
                        doWork(key, val);
                }
                else if (batchList.size() == TestsHelper.getBulkOperationSize() ||
                    batchMap.size() == TestsHelper.getBulkOperationSize()) {
                    if (cacheStore != null)
                        doWork(batchList);
                    else
                        doWork(batchMap);

                    batchMap.clear();
                    batchList.clear();
                }

                if (counter == endPosition)
                    counter = startPosition;
                else
                    counter++;

                key = TestsHelper.generateLoadTestsKey(counter);
                val = TestsHelper.generateLoadTestsValue(counter);

                if (batchMode()) {
                    if (cacheStore != null)
                        batchList.add(new CacheEntryImpl(key, val));
                    else
                        batchMap.put(key, val);
                }

                reportStatistics();
            }
        }
        finally {
            warmupFinishTime = warmupFinishTime != 0 ? warmupFinishTime : System.currentTimeMillis();
            finishTime = System.currentTimeMillis();
            reportTestCompletion();
        }
    }

    private void doWork(CacheEntryImpl entry) throws InterruptedException {
        process(cacheStore, entry);
        updateMetrics(1);
    }

    private void doWork(Object key, Object value) throws InterruptedException {
        process(igniteCache, key, value);
        updateMetrics(1);
    }

    private void doWork(Collection<CacheEntryImpl> entries) throws InterruptedException {
        process(cacheStore, entries);
        updateMetrics(entries.size());
    }

    private void doWork(Map entries) throws InterruptedException {
        process(igniteCache, entries);
        updateMetrics(entries.size());
    }

    private int getWarmUpSpeed() {
        if (warmupMsgProcessed == 0)
            return 0;

        long finish = warmupFinishTime != 0 ? warmupFinishTime : System.currentTimeMillis();
        long duration = (finish - warmupStartTime - warmupSleepCount * TestsHelper.getLoadTestsRequestsLatency()) / 1000;

        return duration == 0 ? warmupMsgProcessed : warmupMsgProcessed / (int)duration;
    }

    private void updateMetrics(int itemsProcessed) throws InterruptedException {
        if (warmup)
            warmupMsgProcessed += itemsProcessed;
        else
            msgProcessed += itemsProcessed;

        if (TestsHelper.getLoadTestsRequestsLatency() > 0)
            Thread.sleep(TestsHelper.getLoadTestsRequestsLatency());

        if (warmup)
            warmupSleepCount++;
        else
            sleepCount++;
    }

    private void reportStatistics() {
        // statistics should be reported only every 30 seconds
        if (System.currentTimeMillis() - statReportedTime < 30000)
            return;

        statReportedTime = System.currentTimeMillis();

        int completed = (int)(statReportedTime - testStartTime) * 100 / TestsHelper.getLoadTestsExecutionTime();

        if (warmup) {
            logger.info("Warm up messages processed " + warmupMsgProcessed + ", " +
                "speed " + getWarmUpSpeed() + " msg/sec, " + completed + "% completed");
        }
        else {
            logger.info("Messages processed " + msgProcessed + ", " +
                "speed " + getSpeed() + " msg/sec, " + completed + "% completed");
        }
    }

    private void reportTestCompletion() {
        StringBuilder builder = new StringBuilder();

        if (executionError != null)
            builder.append("Test execution abnormally terminated. ");
        else
            builder.append("Test execution successfully completed. ");

        builder.append("Statistics: ").append(SystemHelper.LINE_SEPARATOR);
        builder.append("Start time: ").append(TIME_FORMATTER.format(testStartTime)).append(SystemHelper.LINE_SEPARATOR);
        builder.append("Finish time: ").append(TIME_FORMATTER.format(finishTime)).append(SystemHelper.LINE_SEPARATOR);
        builder.append("Duration: ").append((finishTime - testStartTime) / 1000).append(" sec")
            .append(SystemHelper.LINE_SEPARATOR);

        if (TestsHelper.getLoadTestsWarmupPeriod() > 0) {
            builder.append("Warm up period: ").append(TestsHelper.getLoadTestsWarmupPeriod() / 1000)
                .append(" sec").append(SystemHelper.LINE_SEPARATOR);
            builder.append("Warm up processed messages: ").append(warmupMsgProcessed).append(SystemHelper.LINE_SEPARATOR);
            builder.append("Warm up processing speed: ").append(getWarmUpSpeed())
                .append(" msg/sec").append(SystemHelper.LINE_SEPARATOR);
        }

        builder.append("Processed messages: ").append(msgProcessed).append(SystemHelper.LINE_SEPARATOR);
        builder.append("Processing speed: ").append(getSpeed()).append(" msg/sec");

        if (executionError != null)
            logger.error(builder.toString(), executionError);
        else
            logger.info(builder.toString());
    }
}
