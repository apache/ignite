/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compute;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.lang.IgniteFuture;

/**
 * This class defines a handler for asynchronous task execution. It's similar in design
 * to standard JDK {@link Future} interface but has improved and easier to use exception
 * hierarchy.
 * @param <R> Type of the task result returning from {@link ComputeTask#reduce(List)} method.
 */
public interface ComputeTaskFuture<R> extends IgniteFuture<R> {
    /**
     * {@inheritDoc}
     *
     * @throws ComputeTaskTimeoutException If task execution timed out.
     */
    @Override public R get();

    /**
     * {@inheritDoc}
     *
     * @throws ComputeTaskTimeoutException If task execution timed out.
     */
    @Override public R get(long timeout, TimeUnit unit);

    /**
     * Gets task session of execution grid task.
     *
     * @return Task session.
     */
    public ComputeTaskSession getTaskSession();
}