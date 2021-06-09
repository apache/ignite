/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.ignite.raft.jraft.util.FileOutputSignalHandler;
import org.apache.ignite.raft.jraft.util.MetricReporter;
import org.apache.ignite.raft.jraft.util.SystemPropertyUtil;
import org.apache.ignite.raft.jraft.util.ThreadPoolMetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ThreadPoolMetricsSignalHandler extends FileOutputSignalHandler {

    private static Logger LOG = LoggerFactory.getLogger(ThreadPoolMetricsSignalHandler.class);

    private static final String DIR = SystemPropertyUtil.get("jraft.signal.thread.pool.metrics.dir", "");
    private static final String BASE_NAME = "thread_pool_metrics.log";

    @Override
    public void handle(final String signalName) {
        try {
            final File file = getOutputFile(DIR, BASE_NAME);

            LOG.info("Printing thread pools metrics with signal: {} to file: {}.", signalName, file);

            try (final PrintStream out = new PrintStream(new FileOutputStream(file, true))) {
                MetricReporter.forRegistry(ThreadPoolMetricRegistry.metricRegistry()) //
                    .outputTo(out) //
                    .build() //
                    .report();
            }
        }
        catch (final IOException e) {
            LOG.error("Fail to print thread pools metrics.", e);
        }
    }
}
