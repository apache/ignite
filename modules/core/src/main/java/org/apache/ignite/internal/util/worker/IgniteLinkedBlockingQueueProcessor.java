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

import java.util.concurrent.LinkedBlockingQueue;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.thread.context.OperationContextSnapshot;
import org.apache.ignite.internal.thread.context.function.OperationContextAwareWrapper;
import org.apache.ignite.internal.worker.WorkersRegistry;

/** */
public abstract class IgniteLinkedBlockingQueueProcessor<T> extends AsynchronousQueueProcessor<T, OperationContextAwareWrapper<T>> {
    /** */
    protected IgniteLinkedBlockingQueueProcessor(
        String igniteInstanceName,
        String workerThreadName,
        IgniteLogger log,
        WorkersRegistry workerReg
    ) {
        super(igniteInstanceName, workerThreadName, log, workerReg, new LinkedBlockingQueue<>());
    }

    /** {@inheritDoc} */
    @Override protected OperationContextAwareWrapper<T> wrapQueueElement(T delegate, OperationContextSnapshot snapshot) {
        return delegate == null ? null : new OperationContextAwareWrapper<>(delegate, snapshot);
    }
}
