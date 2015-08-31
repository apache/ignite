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

package org.apache.ignite.compute;

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.Nullable;

/**
 * This runtime exception can be thrown from {@link ComputeJob#execute()} method to force
 * job failover to another node within task topology. Any
 * {@link IgniteClosure}, {@link Callable}, or {@link Runnable} instance passed into
 * any of the {@link IgniteCompute} methods can also throw this exception to force failover.
 */
public class ComputeJobFailoverException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     */
    public ComputeJobFailoverException(String msg) {
        super(msg);
    }

    /**
     * Creates new given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public ComputeJobFailoverException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates new exception with given error message
     * and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public ComputeJobFailoverException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}