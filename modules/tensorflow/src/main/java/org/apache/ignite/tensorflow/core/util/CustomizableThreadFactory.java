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

package org.apache.ignite.tensorflow.core.util;

import java.util.concurrent.ThreadFactory;
import org.jetbrains.annotations.NotNull;

/**
 * Customizable thread factory that allows to specify thread name and daemon flag for the processes created by this
 * factory.
 */
public class CustomizableThreadFactory implements ThreadFactory {
    /** Thread name. */
    private final String threadName;

    /** Is daemon flag. */
    private final boolean isDaemon;

    /**
     * Constructs a new instance of customizable thread factory.
     *
     * @param threadName Thread name.
     * @param isDaemon Is daemon flag.
     */
    public CustomizableThreadFactory(String threadName, boolean isDaemon) {
        this.threadName = threadName;
        this.isDaemon = isDaemon;
    }

    /** {@inheritDoc} */
    @Override public Thread newThread(@NotNull Runnable r) {
        Thread thread = new Thread(r);

        thread.setName(threadName);
        thread.setDaemon(isDaemon);

        return thread;
    }
}
