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
package org.apache.ignite.raft.jraft.util.concurrent;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.raft.jraft.util.Ints;

/**
 *
 */
public final class DefaultExecutorChooserFactory implements ExecutorChooserFactory {

    public static final DefaultExecutorChooserFactory INSTANCE = new DefaultExecutorChooserFactory();

    @Override
    public ExecutorChooser newChooser(final SingleThreadExecutor[] executors) {
        if (Ints.isPowerOfTwo(executors.length)) {
            return new PowerOfTwoExecutorChooser(executors);
        }
        else {
            return new GenericExecutorChooser(executors);
        }
    }

    private DefaultExecutorChooserFactory() {
    }

    private static class PowerOfTwoExecutorChooser extends AbstractExecutorChooser {

        PowerOfTwoExecutorChooser(SingleThreadExecutor[] executors) {
            super(executors);
        }

        @Override
        public SingleThreadExecutor select(final int index) {
            return this.executors[index & this.executors.length - 1];
        }
    }

    private static class GenericExecutorChooser extends AbstractExecutorChooser {

        protected GenericExecutorChooser(SingleThreadExecutor[] executors) {
            super(executors);
        }

        @Override
        public SingleThreadExecutor select(final int index) {
            return this.executors[Math.abs(index % this.executors.length)];
        }
    }

    private static abstract class AbstractExecutorChooser implements ExecutorChooser {

        protected final AtomicInteger idx = new AtomicInteger();
        protected final SingleThreadExecutor[] executors;

        protected AbstractExecutorChooser(SingleThreadExecutor[] executors) {
            this.executors = executors;
        }

        @Override
        public SingleThreadExecutor next() {
            return select(this.idx.getAndIncrement());
        }
    }
}
