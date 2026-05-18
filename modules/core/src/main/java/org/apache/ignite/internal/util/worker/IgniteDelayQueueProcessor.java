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

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalWrapper;
import org.apache.ignite.internal.thread.context.OperationContextSnapshot;
import org.apache.ignite.internal.thread.context.function.OperationContextAwareWrapper;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** */
public abstract class IgniteDelayQueueProcessor<T extends Delayed>
    extends AsynchronousQueueProcessor<T, IgniteDelayQueueProcessor<T>.QueueElementWrapper> {
    /** */
    protected IgniteDelayQueueProcessor(
        String igniteInstanceName,
        String workerThreadName,
        IgniteLogger log,
        @Nullable WorkersRegistry workerReg
    ) {
        super(igniteInstanceName, workerThreadName, log, workerReg, new DelayQueue<>());
    }

    /** {@inheritDoc} */
    @Override protected QueueElementWrapper wrapQueueElement(T delegate, OperationContextSnapshot snapshot) {
        return new QueueElementWrapper(delegate, snapshot);
    }

    /** */
    protected class QueueElementWrapper extends OperationContextAwareWrapper<T> implements Delayed {
        /** */
        protected QueueElementWrapper(T delegate, OperationContextSnapshot snapshot) {
            super(delegate, snapshot);
        }

        /** {@inheritDoc} */
        @Override public long getDelay(@NotNull TimeUnit unit) {
            return delegate.getDelay(unit);
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull Delayed o) {
            return delegate.compareTo((Delayed)IgniteInternalWrapper.unwrap(o));
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return delegate.equals(IgniteInternalWrapper.unwrap(o));
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return delegate.hashCode();
        }
    }
}
