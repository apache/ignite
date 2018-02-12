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

package org.jsr166;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.LongAdder;

/**
 * {@link ConcurrentLinkedDeque} that uses {@link LongAdder} for fast element counting.
 *
 * @param <E> Deque element type.
 */
public class ConcurrentLinkedDequeEx<E> extends SizeCountingDeque<E, Long> {
    /** */
    private static class Cntr implements SizeCountingDeque.Counter<Long> {
        /** */
        private final LongAdder adder = new LongAdder();

        /** {@inheritDoc} */
        @Override public void inc() {
            adder.increment();
        }

        /** {@inheritDoc} */
        @Override public void dec() {
            adder.decrement();
        }

        /** {@inheritDoc} */
        @Override public void add(int n) {
            adder.add(n);
        }

        /** {@inheritDoc} */
        @Override public Long get() {
            return adder.sum();
        }
    }

    /** {@inheritDoc} */
    public ConcurrentLinkedDequeEx(Deque<E> deque) {
        super(deque, new Cntr());
    }
}
