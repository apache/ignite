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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 *
 */
public class ThreadResolver {
    /** */
    private static final ThreadLocal<Long> THREAD_ID = ThreadLocal.withInitial(() -> -1L);

    /**
     * @param threadId Thread ID.
     */
    public static void setThreadId(long threadId) {
        THREAD_ID.set(threadId);
    }

    /**
     * Reset thread ID previous value.
     */
    public static void reset() {
        THREAD_ID.set(-1L);
    }

    /**
     * @return Thread ID.
     */
    public static long threadId() {
        long id = THREAD_ID.get();

        if (id == -1L)
            THREAD_ID.set(id = Thread.currentThread().getId());

        return id;
    }

    /**
     *
     */
    public static class ThreadLocalExtra<T> extends ThreadLocal<T> {
        /** */
        private final Map<Long, T> threadMap = new ConcurrentHashMap<>();
        /** */
        private static final Object NULL_VALUE = new Object();

        /** {@inheritDoc} */
        @Override protected T initialValue() {
            return super.initialValue();
        }

        /** {@inheritDoc} */
        @Override public void set(T value) {
            threadMap.put(threadId(), value != null ? value : (T)NULL_VALUE);
        }

        /** {@inheritDoc} */
        @Override public T get() {
            T t = threadMap.get(threadId());

            if (t == null) {
                t = initialValue();
                if (t != null)
                    set(t);
            }

            return t == NULL_VALUE ? null : t;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
           threadMap.remove(threadId());
        }

        /**
         * @param supplier Initil value.
         *
         * @return Thread local.
         */
        public static <T> ThreadLocalExtra<T> withInitial(Supplier<? extends T> supplier) {
            return new ThreadLocalExtra<T>() {
                @Override public T get() {
                    T prev = super.get();

                    if (prev == null)
                        set(prev = supplier.get());

                    return prev;
                }
            };
        }
    }
}
