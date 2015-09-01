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

package org.apache.ignite.internal;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.mxbean.ThreadPoolMXBean;

/**
 * Adapter for {@link org.apache.ignite.mxbean.ThreadPoolMXBean} which delegates all method calls to the underlying
 * {@link ExecutorService} instance.
 */
public class ThreadPoolMXBeanAdapter implements ThreadPoolMXBean {
    /** */
    private final ExecutorService exec;

    /**
     * Creates adapter.
     *
     * @param exec Executor service
     */
    public ThreadPoolMXBeanAdapter(ExecutorService exec) {
        assert exec != null;

        this.exec = exec;
    }

    /** {@inheritDoc} */
    @Override public int getActiveCount() {
        return exec instanceof ThreadPoolExecutor ? ((ThreadPoolExecutor)exec).getActiveCount() : -1;
    }

    /** {@inheritDoc} */
    @Override public long getCompletedTaskCount() {
        return exec instanceof ThreadPoolExecutor ? ((ThreadPoolExecutor)exec).getCompletedTaskCount() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getCorePoolSize() {
        return exec instanceof ThreadPoolExecutor ? ((ThreadPoolExecutor)exec).getCorePoolSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getLargestPoolSize() {
        return exec instanceof ThreadPoolExecutor ? ((ThreadPoolExecutor)exec).getLargestPoolSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getMaximumPoolSize() {
        return exec instanceof ThreadPoolExecutor ? ((ThreadPoolExecutor)exec).getMaximumPoolSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getPoolSize() {
        return exec instanceof ThreadPoolExecutor ? ((ThreadPoolExecutor)exec).getPoolSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public long getTaskCount() {
        return exec instanceof ThreadPoolExecutor ? ((ThreadPoolExecutor)exec).getTaskCount() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getQueueSize() {
        return exec instanceof ThreadPoolExecutor ? ((ThreadPoolExecutor)exec).getQueue().size() : -1;
    }

    /** {@inheritDoc} */
    @Override public long getKeepAliveTime() {
        return exec instanceof ThreadPoolExecutor ?
            ((ThreadPoolExecutor)exec).getKeepAliveTime(TimeUnit.MILLISECONDS) : -1;
    }

    /** {@inheritDoc} */
    @Override public boolean isShutdown() {
        return exec.isShutdown();
    }

    /** {@inheritDoc} */
    @Override public boolean isTerminated() {
        return exec.isTerminated();
    }

    /** {@inheritDoc} */
    @Override public boolean isTerminating() {
        return exec instanceof ThreadPoolExecutor && ((ThreadPoolExecutor) exec).isTerminating();
    }

    /** {@inheritDoc} */
    @Override public String getRejectedExecutionHandlerClass() {
        if (!(exec instanceof ThreadPoolExecutor))
            return "";

        RejectedExecutionHandler hnd = ((ThreadPoolExecutor)exec).getRejectedExecutionHandler();

        return hnd == null ? "" : hnd.getClass().getName();
    }

    /** {@inheritDoc} */
    @Override public String getThreadFactoryClass() {
        if (!(exec instanceof ThreadPoolExecutor))
            return "";

        ThreadFactory factory = ((ThreadPoolExecutor)exec).getThreadFactory();

        return factory == null ? "" : factory.getClass().getName();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ThreadPoolMXBeanAdapter.class, this, super.toString());
    }
}