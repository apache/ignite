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

package org.apache.ignite.spi;

import java.util.concurrent.ThreadFactory;
import org.apache.ignite.IgniteLogger;

/**
 * This class provides implementation of {@link ThreadFactory}  factory
 * for creating grid SPI threads.
 */
public class IgniteSpiThreadFactory implements ThreadFactory {
    /** */
    private final IgniteLogger log;

    /** */
    private final String gridName;

    /** */
    private final String threadName;

    /**
     * @param gridName Grid name, possibly {@code null} for default grid.
     * @param threadName Name for threads created by this factory.
     * @param log Grid logger.
     */
    public IgniteSpiThreadFactory(String gridName, String threadName, IgniteLogger log) {
        assert log != null;
        assert threadName != null;

        this.gridName = gridName;
        this.threadName = threadName;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public Thread newThread(final Runnable r) {
        return new IgniteSpiThread(gridName, threadName, log) {
            /** {@inheritDoc} */
            @Override protected void body() {
                r.run();
            }
        };
    }
}