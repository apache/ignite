/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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