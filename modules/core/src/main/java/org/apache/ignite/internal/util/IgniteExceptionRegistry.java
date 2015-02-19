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

package org.apache.ignite.internal.util;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.IgniteSystemProperties.*;

/**
 * Class collects errors from {@link TcpCommunicationSpi} and {@link TcpDiscoverySpi}.
 */
public class IgniteExceptionRegistry {
    /** */
    public static final int DEFAULT_QUEUE_SIZE = 1000;

    /** */
    private int maxSize = IgniteSystemProperties.getInteger(IGNITE_EXCEPTION_REGISTRY_MAX_SIZE, DEFAULT_QUEUE_SIZE);

    /** */
    private AtomicLong errorCnt = new AtomicLong();

    /** */
    private final ConcurrentLinkedDeque<IgniteExceptionInfo> queue = new ConcurrentLinkedDeque<>();

    /** */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param log Ignite logger.
     */
    public IgniteExceptionRegistry(IgniteLogger log) {
        this.log = log;
    }

    /**
     * Puts exception into queue.
     * Thread-safe.
     *
     * @param e Exception.
     */
    public void onException(String msg, Throwable e) {
        errorCnt.incrementAndGet();

        // Remove extra entity.
        while (queue.size() >= maxSize)
            queue.pollLast();

        queue.offerFirst(new IgniteExceptionInfo(e, msg, Thread.currentThread().getId(), 
            Thread.currentThread().getName(), U.currentTimeMillis()));
    }

    /**
     * Gets exceptions.
     *
     * @return Exceptions.
     */
    Collection<IgniteExceptionInfo> getErrors() {
        int size = queue.size();

        List<IgniteExceptionInfo> errors = new ArrayList<>(size);

        int cnt = 0;

        for (IgniteExceptionInfo entry : queue) {
            if (cnt < size)
                errors.add(entry);
            else
                break;
            
            ++cnt;
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
     */
    public void printErrors() {
        int size = queue.size();

        int cnt = 0;

        Iterator<IgniteExceptionInfo> descIter = queue.descendingIterator();
        
        while (descIter.hasNext() && cnt < size){
            IgniteExceptionInfo error = descIter.next();

            log.error(
                "Time of occurrence: " + new Date(error.time()) + "\n" +
                "Error message: " + error.message() + "\n" +
                "Thread id: " + error.threadId() + "\n" +
                "Thread name: " + error.threadName(),
                error.exception()
            );

            ++cnt;
        }
    }

    /**
     * Errors count.
     *
     * @return Errors count.
     */
    public long errorCount() {
        return errorCnt.get();
    }

    /**
     *
     */
    static class IgniteExceptionInfo {
        /** */
        @GridToStringExclude
        private final Throwable exception;

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
         * @param exception Exception.
         * @param threadId Thread id.
         * @param threadName Thread name.
         * @param time Occurrence time.
         */
        public IgniteExceptionInfo(Throwable exception, String msg, long threadId, String threadName, long time) {
            this.exception = exception;
            this.threadId = threadId;
            this.threadName = threadName;
            this.time = time;
            this.msg = msg;
        }

        /**
         * @return Gets message.
         */
        public String message() {
            return msg;
        }

        /**
         * @return Exception.
         */
        public Throwable exception() {
            return exception;
        }

        /**
         * @return Gets thread id.
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
        public String toString() {
            return S.toString(IgniteExceptionInfo.class, this);
        }
    }
}
