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

package org.apache.ignite.internal.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_EXCEPTION_REGISTRY_MAX_SIZE;

/**
 * Utility to collect suppressed errors within internal code.
 */
public class IgniteExceptionRegistry {
    /** */
    public static final int DEFAULT_QUEUE_SIZE = 1000;

    /** */
    private static final IgniteExceptionRegistry instance = new IgniteExceptionRegistry();

    /** */
    private int maxSize = IgniteSystemProperties.getInteger(IGNITE_EXCEPTION_REGISTRY_MAX_SIZE, DEFAULT_QUEUE_SIZE);

    /** */
    private AtomicLong errCnt = new AtomicLong();

    /** */
    private final ConcurrentLinkedDeque<ExceptionInfo> q = new ConcurrentLinkedDeque<>();

    /**
     * @return Registry instance.
     */
    public static IgniteExceptionRegistry get() {
        return instance;
    }

    /**
     *
     */
    private IgniteExceptionRegistry() {
        // No-op.
    }

    /**
     * Puts exception into queue.
     * Thread-safe.
     *
     * @param msg Message that describe reason why error was suppressed.
     * @param e Exception.
     */
    public void onException(String msg, Throwable e) {
        q.offerFirst(
            new ExceptionInfo(
                errCnt.incrementAndGet(),
                e,
                msg,
                Thread.currentThread().getId(),
                Thread.currentThread().getName(),
                U.currentTimeMillis()));

        // Remove extra entries.
        int delta = q.size() - maxSize;

        for (int i = 0; i < delta && q.size() > maxSize; i++)
            q.pollLast();
    }

    /**
     * Gets suppressed errors.
     *
     * @param order Order number to filter errors.
     * @return List of exceptions that happened after specified order.
     */
    public List<ExceptionInfo> getErrors(long order) {
        List<ExceptionInfo> errors = new ArrayList<>();

        for (ExceptionInfo error : q) {
            if (error.order > order)
                errors.add(error);
        }

        return errors;
    }

    /**
     * Sets max size. Default value {@link #DEFAULT_QUEUE_SIZE}
     *
     * @param maxSize Max size.
     */
    public void setMaxSize(int maxSize) {
        A.ensure(maxSize > 0, "Max queue size must be greater than 0.");

        this.maxSize = maxSize;
    }

    /**
     * Prints errors.
     *
     * @param log Logger.
     */
    public void printErrors(IgniteLogger log) {
        int size = q.size();

        Iterator<ExceptionInfo> descIter = q.descendingIterator();

        for (int i = 0; i < size && descIter.hasNext(); i++) {
            ExceptionInfo error = descIter.next();

            U.error(
                log,
                "Error: " + (i + 1) + U.nl() +
                "    Time: " + new Date(error.time()) + U.nl() +
                "    Error: " + error.message() + U.nl() +
                "    Thread ID: " + error.threadId() + U.nl() +
                "    Thread name: " + error.threadName(),
                error.error()
            );
        }
    }

    /**
     * Errors count.
     *
     * @return Errors count.
     */
    public long errorCount() {
        return errCnt.get();
    }

    /**
     * Detailed info about suppressed error.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class ExceptionInfo implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final long order;

        /** */
        @GridToStringExclude
        private final Throwable error;

        /** */
        private final long threadId;

        /** */
        private final String threadName;

        /** */
        private final long time;

        /** */
        private String msg;

        /**
         * Constructor.
         *
         * @param order Locally unique ID that is atomically incremented for each new error.
         * @param error Suppressed error.
         * @param msg Message that describe reason why error was suppressed.
         * @param threadId Thread ID.
         * @param threadName Thread name.
         * @param time Occurrence time.
         */
        public ExceptionInfo(long order, Throwable error, String msg, long threadId, String threadName, long time) {
            this.order = order;
            this.error = error;
            this.threadId = threadId;
            this.threadName = threadName;
            this.time = time;
            this.msg = msg;
        }

        /**
         * @return Locally unique ID that is atomically incremented for each new error.
         */
        public long order() {
            return order;
        }

        /**
         * @return Gets message that describe reason why error was suppressed.
         */
        public String message() {
            return msg;
        }

        /**
         * @return Suppressed error.
         */
        public Throwable error() {
            return error;
        }

        /**
         * @return Gets thread ID.
         */
        public long threadId() {
            return threadId;
        }

        /**
         * @return Gets thread name.
         */
        public String threadName() {
            return threadName;
        }

        /**
         * @return Gets time.
         */
        public long time() {
            return time;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ExceptionInfo.class, this);
        }
    }
}
